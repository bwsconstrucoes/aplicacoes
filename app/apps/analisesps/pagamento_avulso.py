# -*- coding: utf-8 -*-
"""
PAGAMENTO AVULSO — 15/10/2026.

O dono: *"eu quero poder gerar um arquivo de pagamento, SomaPay ou BeeVale (…)
pagamento avulso (…) vou colocar uma lista, uma tabela com CPF e valor (…) e a
obra também, para a gente poder apropriar na obra correta (…) se eu não
determinar, (…) aquela mesma coluninha da obra do ponto, a obra de cadastro (…)
aqueles alertas, se a pessoa já saiu, está saindo, desligado, não tem ponto (…)
a dinâmica toda é igual (…) a diferença basicamente é a fonte dos dados."*

O caminho é o da DC (Despesas com colaboradores), que também paga uma lista
pronta em vez de um cálculo:

1. **A fonte é o que se cola** na tela: uma linha por pagamento, com CPF, valor
   e (opcional) obra, em qualquer ordem — separados por tabulação (colado de
   planilha), ponto e vírgula ou espaço. `ler_texto` reconhece cada pedaço.
2. **Cada linha ganha o resto do cadastro**: nome, função, situação (saiu,
   saindo, afastado), a obra do cadastro e a do PONTO (a de mais dias nos
   últimos 15 dias do mês, a mesma régua dos auxílios), a conta de pagamento
   e o código OMIE pela obra ("C. Diários").
3. **A obra que paga**: a colada; sem ela, a do ponto; sem ponto, a do
   cadastro — e pode ser trocada na linha. A tela diz de onde veio.
4. **Gerar** é o mesmo de todas as folhas (destino por conta, prévia,
   definitivo, relatório PDF e cadastro por conta, análise), e o que foi gerado
   fica num LOTE — é o que alimenta a SP do Pipefy pela aba Arquivos gerados.

⚠️ GUARDADO EM `analisesps.meta`, sem migração: a lista atual (uma só, da
equipe) e o lote de cada geração. São textos curtos — CPF, valor, obra —, nunca
arquivo. Os arquivos gerados vão para o Google Drive, como os das outras folhas.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from decimal import Decimal

from . import colaboradores, formatos
from .folha_rateio import cpf_bonito, so_digitos

logger = logging.getLogger("analisesps.avulso")

TIPO = "avulso"          # o "pagamento" no registro de arquivos
VERBA = "avulso"
CHAVE_DA_LISTA = "pagamento_avulso"
CHAVE_DO_LOTE = "pagamento_avulso_lote:{}"
TETO_DE_LINHAS = 2_000
CENTAVO = Decimal("0.01")

# O tipo de despesa do pagamento: vai para a SP do Pipefy (tipo de despesa e
# categoria do OMIE, procurados pelo nome no "Plano Financeiro", como nas outras
# folhas) e decide a carteira do BeeVale (a tabela do dono, `folha_geracao`).
TIPOS_DE_DESPESA = {
    "Gratificações e Extras": "Gratiticações e Extras",   # ⚠️ assim na tabela dele
    "Salários e Ordenados": "Diárias",
    "Despesas com Alimentação": "Auxílio Alimentação",
    "Despesas com Transporte": "Despesas com Transporte",
}
TIPO_PADRAO = "Gratificações e Extras"

ORIGEM_DA_OBRA = {"voce": "trocada por você", "colada": "colada na lista",
                  "ponto": "do ponto", "cadastro": "do cadastro"}


class ErroDoAvulso(RuntimeError):
    """A frase vai inteira para a tela."""


# ---------------------------------------------------------------------------
# Ler o que foi colado
# ---------------------------------------------------------------------------
_CPF_PONTUADO = re.compile(r"^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$")
_VALOR = re.compile(r"^-?(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{1,2})?$|^-?\d+\.\d{1,2}$")


def _limpo(pedaco: str) -> str:
    return pedaco.strip().strip('"').strip()


def ler_linha(texto: str) -> dict:
    """`{"cpf", "valor", "obra", "erro"}` de UMA linha colada.

    Os pedaços podem vir em qualquer ordem. CPF é o pedaço com 11 dígitos
    (pontuado ou não; com 9 ou 10, quando a planilha comeu os zeros da frente,
    é completado); valor é o número com centavos (1.500,00 · 1500,00 · 1500.00
    · 1500); o que sobra é a obra."""
    pedacos = [_limpo(p) for p in re.split(r"[\t;|]+|\s+", str(texto or "")) if _limpo(p)]
    pedacos = [p for p in pedacos if p.upper() not in ("R$", "RS")]
    cpf, valor, obra, completado = "", None, [], False
    numeros_puros = [p for p in pedacos if p.isdigit()]
    for p in pedacos:
        digitos = so_digitos(p)
        if not cpf and _CPF_PONTUADO.match(p) and len(digitos) == 11:
            cpf = digitos
            continue
        if (not cpf and p.isdigit() and 9 <= len(p) <= 11
                and (len(numeros_puros) > 1 or any("," in x or "." in x for x in pedacos))):
            cpf, completado = p.zfill(11), len(p) < 11
            continue
        sem_rs = re.sub(r"^R\$\s*", "", p, flags=re.IGNORECASE)
        if valor is None and _VALOR.match(sem_rs):
            valor = formatos.para_numero(sem_rs)
            continue
        obra.append(p)
    erro = ""
    if not cpf:
        erro = "não achei o CPF (11 dígitos)."
    elif valor is None:
        erro = "não achei o valor."
    return {"cpf": cpf, "valor": (Decimal(str(valor)).quantize(CENTAVO)
                                  if valor is not None else None),
            "obra": " ".join(obra).upper(), "erro": erro,
            "cpf_completado": completado}


def ler_texto(texto: str) -> tuple:
    """(linhas boas, linhas recusadas) do texto colado. Cabeçalho ("CPF",
    "Valor", "Obra") e linha vazia são ignorados."""
    boas, recusadas = [], []
    for n, bruta in enumerate(str(texto or "").splitlines(), start=1):
        if not bruta.strip():
            continue
        if not re.search(r"\d", bruta) and re.search(r"cpf|valor|obra", bruta, re.I):
            continue                         # o cabeçalho colado junto
        lida = ler_linha(bruta)
        if lida["erro"]:
            recusadas.append({"linha": n, "texto": bruta.strip()[:120], "erro": lida["erro"]})
            continue
        lida["texto"] = bruta.strip()[:200]
        lida["ordem"] = len(boas) + 1
        lida["chave"] = hashlib.sha1(
            f"{lida['ordem']}|{lida['cpf']}|{lida['valor']}|{lida['obra']}".encode()
        ).hexdigest()[:12]
        boas.append(lida)
        if len(boas) > TETO_DE_LINHAS:
            raise ErroDoAvulso(f"a lista passou de {TETO_DE_LINHAS} linhas — divida em partes.")
    return boas, recusadas


# ---------------------------------------------------------------------------
# A lista guardada (uma só, da equipe)
# ---------------------------------------------------------------------------
def _meta_ler(chave: str) -> dict | None:
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?", (chave,))
        return json.loads(linha[0]) if linha and linha[0] else None
    except Exception:  # noqa: BLE001 — lista ruim não derruba a tela
        logger.exception("Avulso: não consegui ler %s", chave)
        return None


def _meta_gravar(chave: str, valor) -> None:
    from .db import conexao
    from .sincronizacao import _meta_gravar as gravar
    with conexao() as conn:
        gravar(conn, chave, json.dumps(valor, ensure_ascii=False, default=str))


def lista() -> dict | None:
    return _meta_ler(CHAVE_DA_LISTA)


def gravar_lista(texto: str, descricao: str = "", tipo_despesa: str = "",
                 quem: str = "") -> dict:
    """Lê o texto colado e o guarda como a lista atual (substitui a anterior)."""
    from .horario import agora
    boas, recusadas = ler_texto(texto)
    if not boas:
        raise ErroDoAvulso("nenhuma linha com CPF e valor no que foi colado."
                           + (f" ({len(recusadas)} recusada(s) — veja o motivo de cada uma.)"
                              if recusadas else ""))
    tipo = tipo_despesa if tipo_despesa in TIPOS_DE_DESPESA else TIPO_PADRAO
    nova = {"texto": str(texto)[:200_000],
            "linhas": [{k: (str(v) if isinstance(v, Decimal) else v) for k, v in l.items()}
                       for l in boas],
            "recusadas": recusadas, "descricao": " ".join(str(descricao or "").split())[:120],
            "tipo_despesa": tipo, "por": str(quem or "")[:80],
            "em": agora().isoformat(), "ajustes": {}, "gerado": None}
    _meta_gravar(CHAVE_DA_LISTA, nova)
    logger.info("Avulso: %s colou %d linha(s) (%d recusada(s)).", quem or "?", len(boas),
                len(recusadas))
    return nova


def limpar_lista(quem: str = "") -> None:
    from .db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.meta WHERE chave = ?", (CHAVE_DA_LISTA,))
        conn.commit()
    logger.info("Avulso: %s limpou a lista.", quem or "?")


def _ajustar(chave: str, campo: str, valor) -> None:
    atual = lista()
    if not atual:
        raise ErroDoAvulso("não há lista colada.")
    if atual.get("gerado"):
        raise ErroDoAvulso("esta lista já foi gerada — cole uma nova.")
    if chave not in {l["chave"] for l in atual["linhas"]}:
        raise ErroDoAvulso("linha não encontrada (a lista foi trocada?).")
    aj = atual.setdefault("ajustes", {}).setdefault(chave, {})
    if valor in (None, ""):
        aj.pop(campo, None)
    else:
        aj[campo] = valor
    _meta_gravar(CHAVE_DA_LISTA, atual)


def escolher_obra(chave: str, obra: str, quem: str = "") -> None:
    _ajustar(chave, "obra", " ".join(str(obra or "").split()).upper())


def salvar_selecao(decisoes, quem: str = "") -> dict:
    atual = lista()
    if not atual:
        raise ErroDoAvulso("não há lista colada.")
    if atual.get("gerado"):
        raise ErroDoAvulso("esta lista já foi gerada — cole uma nova.")
    chaves = {l["chave"] for l in atual["linhas"]}
    ajustes = atual.setdefault("ajustes", {})
    mudou = 0
    for d in decisoes or []:
        chave = str((d or {}).get("chave") or "")
        if chave in chaves:
            ajustes.setdefault(chave, {})["pagar"] = bool(d.get("pagar"))
            mudou += 1
    _meta_gravar(CHAVE_DA_LISTA, atual)
    return {"salvas": mudou}


# ---------------------------------------------------------------------------
# A lista enriquecida — o que a tela mostra e o arquivo leva
# ---------------------------------------------------------------------------
def _obras_do_ponto(hoje) -> tuple:
    """(`{cpf: {"obra", "dias"}}`, `{cpf: [dias]}`) — o ponto do mês corrente;
    sem ele, o do anterior. Nunca estoura: sem ponto, dicionários vazios."""
    from . import folha_auxilio as fx
    try:
        ano, mes = hoje.year, hoje.month
        dias = fx._ponto_do_mes(ano, mes)
        if not dias:
            ano, mes = (ano - 1, 12) if mes == 1 else (ano, mes - 1)
            dias = fx._ponto_do_mes(ano, mes)
        inicio, fim = fx.periodo_do_mes(ano, mes)
        obras = fx._obra_do_ponto_por_cpf(ano, mes, inicio, fim, dias)
        obras.pop("_janela", None)
        return obras, dias or {}
    except Exception:  # noqa: BLE001 — o ponto é apoio aqui
        logger.exception("Avulso: não consegui ler o ponto")
        return {}, {}


def calcular() -> dict:
    """A lista colada, pronta para a tela e para o arquivo."""
    from . import folha_cards, folha_pagamento as fpg
    from .horario import agora

    base = {"pessoas": [], "avisos": [], "total": Decimal("0.00"), "quantos": 0,
            "quantos_a_pagar": 0, "com_problema": [], "lista": None,
            "recusadas": [], "gerado": None}
    atual = lista()
    if not atual:
        return base
    base.update({"lista": atual, "recusadas": atual.get("recusadas") or [],
                 "gerado": atual.get("gerado"), "descricao": atual.get("descricao") or "",
                 "tipo_despesa": atual.get("tipo_despesa") or TIPO_PADRAO})
    linhas = atual.get("linhas") or []
    hoje = agora().date()
    cpfs = sorted({l["cpf"] for l in linhas})
    fichas = colaboradores.muitos_por_cpf(cpfs) if cpfs else {}
    por_nome = colaboradores.codigos_das_obras()
    contas = fpg.conta_por_obra()
    omie = folha_cards._codigos_omie()
    obras_ponto, dias_ponto = _obras_do_ponto(hoje)
    tem_ponto = bool(dias_ponto)
    if not tem_ponto:
        base["avisos"].append("Sem ponto carregado neste mês nem no anterior: a obra "
                              "do ponto não aparece, e o alerta \"sem ponto\" não é dado.")
    ajustes = atual.get("ajustes") or {}
    vezes = {}
    for l in linhas:
        vezes[l["cpf"]] = vezes.get(l["cpf"], 0) + 1
    tipo = base["tipo_despesa"]
    carteira = TIPOS_DE_DESPESA.get(tipo, TIPOS_DE_DESPESA[TIPO_PADRAO])
    gerada = bool(atual.get("gerado"))

    pessoas = []
    for l in linhas:
        ficha = fichas.get(l["cpf"]) or {}
        aj = ajustes.get(l["chave"]) or {}
        valor = Decimal(str(l.get("valor") or 0)).quantize(CENTAVO)
        obra_ponto = ((obras_ponto.get(l["cpf"]) or {}).get("obra") or "").upper()
        obra_cadastro = (colaboradores.resolver_obra(ficha, por_nome) or "").upper() if ficha else ""
        colada = l.get("obra") or ""
        if aj.get("obra"):
            obra, de_onde = aj["obra"], "voce"
        elif colada:
            obra, de_onde = colada, "colada"
        elif obra_ponto:
            obra, de_onde = obra_ponto, "ponto"
        else:
            obra, de_onde = obra_cadastro, ("cadastro" if obra_cadastro else "")
        p = {
            "chave": l["chave"], "ordem": l.get("ordem"), "texto": l.get("texto", ""),
            "cpf": l["cpf"], "cpf_bonito": cpf_bonito(l["cpf"]),
            "nome": ficha.get("nome") or "", "cargo": ficha.get("cargo") or "",
            "tipo_contrato": ficha.get("tipo_contrato") or ficha.get("tipo") or "",
            "fase": ficha.get("fase") or "", "situacao": ficha.get("situacao") or "",
            "link_pipefy": ficha.get("link_pipefy") or "",
            "obra": obra, "obra_de_onde": de_onde,
            "rotulo_obra": ORIGEM_DA_OBRA.get(de_onde, ""),
            "obra_colada": colada, "obra_ponto": obra_ponto,
            "dias_na_obra": (obras_ponto.get(l["cpf"]) or {}).get("dias") or 0,
            "obra_cadastro": obra_cadastro, "obra_trocada": bool(aj.get("obra")),
            "obras": [obra] if obra else [],
            "conta": contas.get(obra, ""), "omie": omie.get(folha_cards._chave(obra), ""),
            "tipo_despesa": tipo, "carteira": carteira, "valor": valor,
            "gerada": gerada, "motivos": [], "impossivel": False,
        }
        m = p["motivos"]
        if not ficha:
            p["impossivel"] = True
            m.append("CPF fora do cadastro de colaboradores — sem o nome, o banco "
                     "recusa o pagamento.")
        if valor <= 0:
            p["impossivel"] = True
            m.append("valor zerado ou negativo.")
        if l.get("cpf_completado"):
            m.append("CPF completado com zero à esquerda (a planilha tinha comido o zero) "
                     "— confira.")
        if ficha:
            if ficha.get("situacao") == colaboradores.SITUACAO_SAIU:
                m.append(ficha.get("motivo") or "colaborador desligado no cadastro.")
            elif ficha.get("situacao") == colaboradores.SITUACAO_SAINDO:
                m.append(ficha.get("motivo") or "colaborador em desligamento.")
            elif ficha.get("situacao") == colaboradores.SITUACAO_AFASTADO:
                m.append("colaborador afastado no cadastro.")
            if ficha.get("desacordo"):
                m.append(ficha["desacordo"])
            if tem_ponto and l["cpf"] not in dias_ponto:
                m.append("sem ponto no mês.")
        if vezes.get(l["cpf"], 0) > 1:
            m.append(f"este CPF aparece {vezes[l['cpf']]} vezes na lista — no arquivo, "
                     "as linhas dele viram uma, somando.")
        if colada and colada not in contas and not omie.get(folha_cards._chave(colada)):
            m.append(f'a obra colada "{colada}" não está na aba "C. Diários" — confira o código.')
        if not obra:
            m.append("sem obra: cole a obra na lista ou escolha na linha.")
        elif not p["conta"]:
            m.append(f'a obra {obra} não tem conta de pagamento na aba "C. Diários".')
        elif not p["omie"]:
            m.append(f'a obra {obra} não tem Código Omie na aba "C. Diários".')
        if colada and obra_ponto and colada != obra_ponto and de_onde == "colada":
            m.append(f"o ponto diz {obra_ponto} — a obra que paga é a colada ({colada}).")
        desligado = ficha.get("situacao") == colaboradores.SITUACAO_SAIU
        p["pagar_calculado"] = not p["impossivel"] and bool(p["conta"]) and not desligado
        p["pagar"] = (p["pagar_calculado"] if aj.get("pagar") is None
                      else bool(aj["pagar"]) and not p["impossivel"])
        p["ajuste_pagar"] = aj.get("pagar")
        pessoas.append(p)

    pessoas.sort(key=lambda p: (p["pagar"], not p["motivos"], (p["nome"] or "").lower(),
                                p["ordem"] or 0))
    a_pagar = calcular_pessoas_a_pagar({"pessoas": pessoas})
    base["resumos"] = {campo: resumo_por(a_pagar, campo) for campo in ("obra", "conta")}
    base.update({
        "pessoas": pessoas, "quantos": len(pessoas), "quantos_a_pagar": len(a_pagar),
        "total": sum((p["valor"] for p in a_pagar), Decimal("0.00")),
        "com_problema": [p for p in pessoas if p["impossivel"] or not p["conta"]],
        "alertas": [p for p in pessoas if p["motivos"] and not p["impossivel"]],
        "pronto": True,
    })
    return base


AGRUPAMENTOS = [("obra", "Obra"), ("conta", "Conta"), ("", "Sem agrupar")]
AGRUPAMENTO_PADRAO = ""


def _rotulo_do_grupo(p: dict, campo: str) -> str:
    return p.get(campo) or {"obra": "(sem obra)", "conta": "(sem conta)"}.get(campo, "—")


def resumo_por(linhas, campo: str) -> list:
    grupos: dict = {}
    for p in linhas:
        rotulo = _rotulo_do_grupo(p, campo)
        g = grupos.setdefault(rotulo, {"rotulo": rotulo, "linhas": 0, "cpfs": set(),
                                       "total": Decimal("0.00")})
        g["linhas"] += 1
        g["cpfs"].add(p["cpf"])
        g["total"] += p["valor"]
    return [{"rotulo": g["rotulo"], "linhas": g["linhas"], "pessoas": len(g["cpfs"]),
             "total": g["total"]}
            for g in sorted(grupos.values(), key=lambda g: (-g["total"], g["rotulo"]))]


def agrupar(pessoas, campo: str) -> list:
    from . import folha_lista
    return folha_lista.agrupar(pessoas, campo, rotulo=_rotulo_do_grupo,
                               pendente=lambda p: p["impossivel"] or not p["conta"])


def calcular_pessoas_a_pagar(calculado: dict) -> list:
    return [p for p in calculado["pessoas"]
            if p["pagar"] and p["valor"] > 0 and not p["gerada"]]


def linhas_a_pagar(calculado: dict | None = None) -> list:
    """As linhas marcadas, no formato da geração (`folha_pagamento.gerar`)."""
    calculado = calcular() if calculado is None else calculado
    return [{"cpf": p["cpf"], "nome": p["nome"], "obra": p["obra"], "conta": p["conta"],
             "verba": VERBA, "valor": p["valor"], "dias": 0, "origem": VERBA,
             "natureza": p["tipo_despesa"], "carteira": p["carteira"],
             "chave": p["chave"], "tipo_despesa": p["tipo_despesa"]}
            for p in calcular_pessoas_a_pagar(calculado)]


# ---------------------------------------------------------------------------
# O LOTE — o que cada geração levou (é o que a SP do Pipefy lê)
# ---------------------------------------------------------------------------
def registrar_lote(analise_id, linhas, quem: str = "") -> None:
    from .horario import agora
    atual = lista() or {}
    lote = {"analise_id": analise_id, "por": str(quem or "")[:80],
            "em": agora().isoformat(),
            "descricao": atual.get("descricao") or "",
            "tipo_despesa": atual.get("tipo_despesa") or TIPO_PADRAO,
            "linhas": [{"cpf": l["cpf"], "nome": l["nome"], "obra": l["obra"],
                        "conta": l["conta"], "valor": str(l["valor"])} for l in linhas]}
    _meta_gravar(CHAVE_DO_LOTE.format(int(analise_id)), lote)
    if atual:
        atual["gerado"] = {"analise_id": analise_id, "em": lote["em"], "por": lote["por"]}
        _meta_gravar(CHAVE_DA_LISTA, atual)


def lote_da_analise(analise_id: int) -> dict | None:
    lote = _meta_ler(CHAVE_DO_LOTE.format(int(analise_id)))
    if not lote:
        return None
    for l in lote.get("linhas") or []:
        l["valor"] = Decimal(str(l.get("valor") or 0))
    return lote


def apagar_lotes(analise_ids) -> int:
    """Excluir a geração em Arquivos gerados devolve a lista para gerar de novo."""
    from .db import conexao
    ids = [int(i) for i in analise_ids or [] if str(i).strip().isdigit()]
    if not ids:
        return 0
    with conexao() as conn:
        cur = conn.execute(
            "DELETE FROM analisesps.meta WHERE chave IN ("
            + ", ".join("?" for _ in ids) + ")",
            tuple(CHAVE_DO_LOTE.format(i) for i in ids))
        n = cur.rowcount or 0
        cur.close()
        conn.commit()
    atual = lista()
    if atual and (atual.get("gerado") or {}).get("analise_id") in ids:
        atual["gerado"] = None
        _meta_gravar(CHAVE_DA_LISTA, atual)
    return n


# ---------------------------------------------------------------------------
# O RELATÓRIO — o mesmo das outras folhas
# ---------------------------------------------------------------------------
def montado_do_relatorio(calculado: dict, pessoas=None, filtros=None,
                         so_a_pagar: bool = False) -> dict:
    from .horario import agora
    pessoas = calculado["pessoas"] if pessoas is None else pessoas
    linhas = []
    for p in pessoas:
        entra = bool(p["pagar"] and p["valor"] > 0)
        if so_a_pagar and not entra:
            continue
        linhas.append({
            "entra": entra, "cpf": p["cpf"], "nome_na_tela": p["nome"] or p["cpf_bonito"],
            "cargo": p["cargo"], "fase": p["fase"],
            "por_obra": [{"obra": p["obra"], "dias": 0, "valor": p["valor"]}],
            "por_dia": [], "dias_no_ponto": 0, "valor": p["valor"], "valor_por_dia": None,
            "obras_resumo": p["obra"] or "-", "obra_do_cadastro": p["obra_cadastro"],
            "contas": [p["conta"]] if p["conta"] else [],
            "situacao_rotulo": f"{p['tipo_despesa']} · obra {p['rotulo_obra'] or '—'}"})
    hoje = agora().strftime("%d/%m/%Y")
    descricao = calculado.get("descricao") or ""
    return {"titulo": f"Pagamento avulso {hoje}" + (f" — {descricao}" if descricao else ""),
            "prefixo_arquivo": "Pagamento avulso",
            "folha": {"competencia": hoje,
                      "rotulo_do_tipo": descricao or calculado.get("tipo_despesa") or ""},
            "pessoas": linhas, "filtros_texto": [], "filtros": filtros or {},
            "totais": {"pessoas": len(pessoas)}, "fechamento": None}
