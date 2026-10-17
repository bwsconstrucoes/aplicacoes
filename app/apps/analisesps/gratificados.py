# -*- coding: utf-8 -*-
"""
GRATIFICADOS E MENSALISTAS, E O PAGAMENTO VIA SP — 17/10/2026.

O dono, pedindo as duas telas: *"a tela de gratificados e a tela de PJ e
Pró-labore (…) a gente pode chamar eles até de pagamento via SP (…) quando for
gerar, lançar no Pipefy, cada um vai ter que ser gerado individualizado, cada
pagamento (…) um SP por pessoa (…) cada um com o seu rateio (…) a gente poder
adicionar um desconto ou um acréscimo (…) ter a visão do todo e do que está
acontecendo naquela quinzena e o que está acontecendo na outra."*

QUEM ENTRA é a aba GM da planilha de Diaristas/Extras/GM, que ele mandou:

    QUERY(CadastroColaboradores!A3:V; "Select A, B, F, J, Q, R, S, T
          WHERE I != 'Colaboradores Desligados' and (J = 'Autônomo Mensalista'
          or J = 'PJ' or J = 'Pró-labore' or J = 'Estágio' or P = 'Sim')")

Ou seja: não desligado E (tipo de contrato PJ / Pró-labore / Autônomo
Mensalista / Estágio, OU marcado "Recebe Gratificação Mensal"). As colunas da
aba oculta, traduzidas para o cadastro ("Dados Documentos"): I = Fase Atual,
J = Tipo de Contrato, P = Recebe Gratificação Mensal (BE), Q = Valor da
Gratificação (BF), R = Valor do Mês (BG), S = Recebe Parcela Única (BH) e
T = Recebe via SP (BI).

QUANTO, por período:

- o valor do mês é o da gratificação; sem ele, o "Valor do Mês" (os dois
  preenchidos com valores diferentes viram alerta — ver `valor_do_mes`);
- sem parcela única: METADE na quinzena (dias 1 a 15) e o resto no fim do mês;
- com parcela única: NADA na quinzena e o valor inteiro no fim do mês;
- mais o desconto ou acréscimo daquela pessoa naquele período, com motivo.

PARA ONDE, as duas telas:

- quem tem "Recebe via SP" = Sim (PJ, pró-labore…) vai para a tela
  **Pagamento via SP**: não entra em arquivo BeeVale nem SomaPay — cada pessoa
  vira UMA SP no Pipefy, com o próprio rateio por obra;
- os demais ficam na tela **Gratificados**, que gera o arquivo como as outras
  folhas (BeeVale ou SomaPay por conta).

DE QUAL OBRA: a escolhida na tela (uma obra, ou dividida em percentuais); sem
ela, a regra do "Rateio das obras"; sem regra, a do ponto (mais dias nos
últimos 15 dias do mês); sem ponto, a do cadastro — marcada, porque obra de
cadastro desatualizado tira dinheiro da conta errada. A CONTA de pagamento é a
da obra (a de maior parte, quando há rateio).

⚠️ GUARDADO EM `analisesps.meta`, sem migração: a obra escolhida por pessoa (vale
para todos os meses), os ajustes e a seleção de cada período, e o lote de cada
geração. Os campos do cadastro que faltavam (Valor do Mês, Recebe Gratificação
Mensal, Recebe via SP, credor e Pix) vêm no JSON de documentos da ficha
(migração 047), também sem migração nova.
"""
from __future__ import annotations

import json
import logging
import unicodedata
from decimal import ROUND_DOWN, Decimal

from . import colaboradores, formatos
from .folha_rateio import cpf_bonito, so_digitos

logger = logging.getLogger("analisesps.gratificados")

CENTAVO = Decimal("0.01")

GM = "gm"            # a tela Gratificados (arquivo BeeVale/SomaPay)
SP = "sp"            # a tela Pagamento via SP (uma SP por pessoa)
VIAS = (GM, SP)
VERBA_DA_VIA = {GM: "gratificacao", SP: "viasp"}
ROTULO_DA_VIA = {GM: "Gratificados e mensalistas", SP: "Pagamento via SP"}

PERIODOS = ("quinzena", "fim_de_mes")
ROTULO_DO_PERIODO = {"quinzena": "Quinzena (dias 1 a 15)",
                     "fim_de_mes": "Fim de mês (16 ao último dia)"}

# O tipo de despesa das SPs e a categoria do OMIE — a planilha usa sempre
# "Gratificações e Extras" na GM (docs/FOLHA_DE_PAGAMENTO.md §7.14.8).
TIPO_DE_DESPESA = "Gratificações e Extras"

# Os tipos de contrato que entram sem a marcação de gratificação — os da QUERY,
# comparados sem acento e sem caixa.
TIPOS_QUE_ENTRAM = {"autonomo mensalista", "pj", "pro-labore", "pro labore",
                    "prolabore", "estagio"}

CHAVE_DAS_OBRAS = "gratificados_obras"
CHAVE_DO_PERIODO = "gratificados_periodo:{ano}-{mes:02d}:{periodo}"
CHAVE_DO_LOTE = "gratificados_lote:{}"

RATEIO_A_MAO = "RATEIO:"

ORIGEM_DA_OBRA = {"mao": "escolhida por você", "dividida": "dividida por você",
                  "regra": "regra de rateio", "ponto": "do ponto",
                  "cadastro": "do cadastro"}

# O filtro "Neste período" da lateral.
NO_PERIODO = [("recebe", "recebe neste período"),
              ("so_fim", "só no fim do mês (parcela única)"),
              ("sem_valor", "sem valor no cadastro")]


class ErroDosGratificados(RuntimeError):
    """A frase vai inteira para a tela."""


def _sem_acento(texto) -> str:
    t = unicodedata.normalize("NFD", str(texto or ""))
    return " ".join("".join(c for c in t if unicodedata.category(c) != "Mn")
                    .lower().split())


def _sim(texto) -> bool:
    return _sem_acento(texto).startswith("sim")


def _dinheiro(valor) -> Decimal:
    if valor in (None, ""):
        return Decimal("0.00")
    if isinstance(valor, Decimal):
        return valor.quantize(CENTAVO)
    numero = formatos.para_numero(valor)
    return Decimal(str(numero or 0)).quantize(CENTAVO)


# ---------------------------------------------------------------------------
# AS REGRAS DE DINHEIRO — funções puras, testadas sem banco
# ---------------------------------------------------------------------------
def valor_do_mes(gratificacao, mensal) -> tuple:
    """(valor do mês, de onde veio, alerta).

    A gratificação manda; sem ela, o "Valor do Mês". Os dois preenchidos com
    valores DIFERENTES não têm resposta certa na planilha — vale a gratificação
    e a linha ganha alerta, para alguém olhar o cadastro."""
    g, m = _dinheiro(gratificacao), _dinheiro(mensal)
    if g > 0:
        alerta = (f"o cadastro tem Valor da Gratificação (R$ {formatos.moeda(g)}) e "
                  f"Valor do Mês (R$ {formatos.moeda(m)}) diferentes — vale a "
                  "gratificação; confira o cadastro." if m > 0 and m != g else "")
        return g, "gratificacao", alerta
    if m > 0:
        return m, "mensal", ""
    return Decimal("0.00"), "", ""


def partes_do_mes(valor_mes, parcela_unica: bool) -> tuple:
    """(quinzena, fim do mês). Sem parcela única: metade na quinzena — o centavo
    que sobra da divisão vai para o fim do mês, para as duas somarem o mês."""
    total = _dinheiro(valor_mes)
    if parcela_unica:
        return Decimal("0.00"), total
    quinzena = (total / 2).quantize(CENTAVO, rounding=ROUND_DOWN)
    return quinzena, total - quinzena


def rateio_da_escolha(texto) -> list:
    """`[{obra, percentual}]` de "RATEIO:A=60;B=40" — vazio se não fechar 100."""
    from . import folha_auxilio
    return folha_auxilio.rateio_da_escolha(texto)


def periodo_padrao(hoje) -> tuple:
    """(ano, mês, período) que a tela abre. Até o dia 10, o fim do mês
    anterior (a mesma régua da competência da planilha, §7.14.11); do 11 ao 25,
    a quinzena do mês; depois, o fim do mês."""
    if hoje.day <= 10:
        ano, mes = (hoje.year - 1, 12) if hoje.month == 1 else (hoje.year, hoje.month - 1)
        return ano, mes, "fim_de_mes"
    if hoje.day <= 25:
        return hoje.year, hoje.month, "quinzena"
    return hoje.year, hoje.month, "fim_de_mes"


def entra_na_lista(ficha: dict, docs: dict) -> bool:
    """A QUERY da aba GM: não desligado E (tipo de contrato da lista OU marcado
    para gratificação mensal). Sem a coluna da marcação na carga, vale ter
    valor de gratificação — senão os CLT gratificados sumiriam calados."""
    if colaboradores.fase_diz_desligado(ficha.get("fase")):
        return False
    tipo = _sem_acento(ficha.get("tipo_contrato") or ficha.get("tipo"))
    if tipo in TIPOS_QUE_ENTRAM:
        return True
    if "recebe_gratificacao" in docs:
        return _sim(docs.get("recebe_gratificacao"))
    return _dinheiro(ficha.get("valor_gratificacao")) > 0


def via_da_pessoa(docs: dict) -> str:
    return SP if _sim(docs.get("recebe_via_sp")) else GM


# ---------------------------------------------------------------------------
# O QUE FICA GUARDADO
# ---------------------------------------------------------------------------
def _meta_ler(chave: str) -> dict:
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?", (chave,))
        dados = json.loads(linha[0]) if linha and linha[0] else {}
        return dados if isinstance(dados, dict) else {}
    except Exception:  # noqa: BLE001 — ajuste ilegível não derruba a tela
        logger.exception("Gratificados: não consegui ler %s", chave)
        return {}


def _meta_gravar(chave: str, valor) -> None:
    from .db import conexao
    from .sincronizacao import _meta_gravar as gravar
    with conexao() as conn:
        gravar(conn, chave, json.dumps(valor, ensure_ascii=False, default=str))


def _chave_do_periodo(ano, mes, periodo) -> str:
    if periodo not in PERIODOS:
        raise ErroDosGratificados("período não reconhecido (quinzena ou fim do mês).")
    return CHAVE_DO_PERIODO.format(ano=int(ano), mes=int(mes), periodo=periodo)


def ajustes_do_periodo(ano, mes, periodo) -> dict:
    return _meta_ler(_chave_do_periodo(ano, mes, periodo))


def obras_escolhidas() -> dict:
    return _meta_ler(CHAVE_DAS_OBRAS)


def escolher_obra(cpf: str, obra: str, quem: str = "") -> None:
    """A obra que paga a pessoa, ou "RATEIO:A=60;B=40". Vazio volta à automática.
    Vale para TODOS os períodos — o rateio de um PJ não muda a cada quinzena."""
    cpf = so_digitos(cpf)
    if len(cpf) != 11:
        raise ErroDosGratificados("CPF inválido.")
    obra = " ".join(str(obra or "").split()).upper()
    if obra.startswith(RATEIO_A_MAO) and not rateio_da_escolha(obra):
        raise ErroDosGratificados("a divisão precisa de obras diferentes somando 100%.")
    atual = obras_escolhidas()
    if obra:
        atual[cpf] = {"obra": obra, "por": str(quem or "")[:80]}
    else:
        atual.pop(cpf, None)
    _meta_gravar(CHAVE_DAS_OBRAS, atual)
    logger.info("Gratificados: %s trocou a obra de %s para %s.", quem or "?", cpf,
                obra or "(automática)")


def ajustar(ano, mes, periodo, cpf: str, valor=None, motivo: str = "",
            quem: str = "") -> None:
    """O desconto (negativo) ou acréscimo (positivo) desta pessoa neste
    período, com o motivo. `valor` None tira o ajuste."""
    cpf = so_digitos(cpf)
    if len(cpf) != 11:
        raise ErroDosGratificados("CPF inválido.")
    chave = _chave_do_periodo(ano, mes, periodo)
    atual = _meta_ler(chave)
    dele = atual.setdefault(cpf, {})
    if valor in (None, ""):
        dele.pop("ajuste", None)
        dele.pop("motivo", None)
    else:
        numero = formatos.para_numero(valor)
        if numero is None:
            raise ErroDosGratificados("valor do ajuste não reconhecido.")
        if not str(motivo or "").strip():
            raise ErroDosGratificados("informe o motivo do desconto ou acréscimo.")
        dele["ajuste"] = str(Decimal(str(numero)).quantize(CENTAVO))
        dele["motivo"] = " ".join(str(motivo).split())[:300]
        dele["por"] = str(quem or "")[:80]
    if not dele:
        atual.pop(cpf, None)
    _meta_gravar(chave, atual)


def salvar_selecao(ano, mes, periodo, decisoes, quem: str = "") -> dict:
    chave = _chave_do_periodo(ano, mes, periodo)
    atual = _meta_ler(chave)
    mudou = 0
    for d in decisoes or []:
        cpf = so_digitos((d or {}).get("cpf"))
        if len(cpf) == 11:
            atual.setdefault(cpf, {})["pagar"] = bool(d.get("pagar"))
            mudou += 1
    _meta_gravar(chave, atual)
    return {"salvas": mudou}


# ---------------------------------------------------------------------------
# A LISTA — o que a tela mostra e o arquivo (ou a SP) leva
# ---------------------------------------------------------------------------
def _obras_do_ponto(ano, mes) -> tuple:
    """(`{cpf: {obra, dias}}`, `{cpf: dias}`) do mês. Sem ponto, vazios."""
    from . import folha_auxilio as fx
    try:
        dias = fx._ponto_do_mes(ano, mes)
        inicio, fim = fx.periodo_do_mes(ano, mes)
        obras = fx._obra_do_ponto_por_cpf(ano, mes, inicio, fim, dias)
        obras.pop("_janela", None)
        return obras, dias or {}
    except Exception:  # noqa: BLE001 — o ponto é apoio aqui
        logger.exception("Gratificados: não consegui ler o ponto")
        return {}, {}


def _favorecido(ficha: dict, docs: dict) -> dict:
    """Quem recebe a SP: o credor (PJ) ou a própria pessoa, e a chave Pix — a do
    cadastro; sem ela, o CPF, como a planilha fazia (§7.14.8)."""
    cnpj = so_digitos(docs.get("pix_cnpj"))
    nome = " ".join(str(docs.get("nome_credor") or "").split()) or ficha.get("nome") or ""
    chave = " ".join(str(docs.get("pix_chave") or "").split())
    tipo_chave = " ".join(str(docs.get("pix_tipo") or "").split())
    if not chave and len(cnpj) == 14:
        chave, tipo_chave = cnpj, "CNPJ"
    if not chave:
        chave, tipo_chave = ficha.get("cpf") or "", "CPF"
    return {"nome": nome.upper(), "pj": len(cnpj) == 14,
            "documento": (_cnpj_bonito(cnpj) if len(cnpj) == 14
                          else cpf_bonito(ficha.get("cpf") or "")),
            "chave_pix": chave, "tipo_chave": tipo_chave or "—",
            "chave_do_cpf": not docs.get("pix_chave") and len(cnpj) != 14}


def _cnpj_bonito(d: str) -> str:
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}" if len(d) == 14 else d


def calcular(ano: int, mes: int, periodo: str, via: str = GM) -> dict:
    """A lista de uma das duas telas, num período."""
    from . import folha_auxilio as fx, folha_cards, folha_pagamento as fpg, folha_rateio

    if via not in VIAS:
        raise ErroDosGratificados("tela não reconhecida.")
    if periodo not in PERIODOS:
        raise ErroDosGratificados("selecione quinzena ou fim do mês.")
    ano, mes = int(ano), int(mes)
    inicio, fim = fx.periodo_do_mes(ano, mes)
    fichas = colaboradores.buscar(so_ativos=False, teto=10000, ate=fim)
    docs_por_cpf = {cpf: d.get("documentos") or {} for cpf, d in
                    colaboradores.documentos_de([f["cpf"] for f in fichas]).items()}
    candidatas = [f for f in fichas if entra_na_lista(f, docs_por_cpf.get(f["cpf"], {}))]
    da_outra = [f for f in candidatas
                if via_da_pessoa(docs_por_cpf.get(f["cpf"], {})) != via]
    fichas = [f for f in candidatas
              if via_da_pessoa(docs_por_cpf.get(f["cpf"], {})) == via]

    ajustes = ajustes_do_periodo(ano, mes, periodo)
    escolhidas = obras_escolhidas()
    regras = folha_rateio.regras_ativas_por_cpf()
    por_nome = colaboradores.codigos_das_obras()
    contas = fpg.conta_por_obra()
    omie = folha_cards._codigos_omie()
    obras_ponto, dias_ponto = _obras_do_ponto(ano, mes)
    verba = VERBA_DA_VIA[via]
    gerados = _gerados(ano, mes, periodo, verba)

    avisos = []
    if not dias_ponto:
        avisos.append("Sem ponto carregado neste mês: a obra do ponto não aparece, e "
                      "quem não tem rateio nem obra escolhida usa a do cadastro.")
    sem_coluna = [rotulo for campo, rotulo in (
        ("recebe_via_sp", "Recebe via SP"), ("valor_mensal", "Valor do Mês"),
        ("recebe_gratificacao", "Recebe Gratificação Mensal"))
        if fichas and not any(campo in d for d in docs_por_cpf.values())]
    if sem_coluna:
        avisos.append("O cadastro ainda não trouxe a(s) coluna(s) " + ", ".join(
            f'"{c}"' for c in sem_coluna) + " — clique em Atualizar cadastro na tela "
            "Colaboradores. Até lá, quem é pago via SP pode aparecer na tela errada.")

    pessoas = []
    for ficha in fichas:
        cpf = ficha["cpf"]
        docs = docs_por_cpf.get(cpf, {})
        aj = ajustes.get(cpf) or {}
        base, de_onde_valor, alerta_valor = valor_do_mes(
            ficha.get("valor_gratificacao"), docs.get("valor_mensal"))
        unica = _sim(ficha.get("parcela_unica"))
        q, f = partes_do_mes(base, unica)
        do_periodo = q if periodo == "quinzena" else f
        ajuste = _dinheiro(aj.get("ajuste")) if aj.get("ajuste") not in (None, "") else Decimal("0.00")
        valor = do_periodo + ajuste

        # A obra: escolhida (ou dividida) > regra de rateio > ponto > cadastro.
        do_ponto = obras_ponto.get(cpf) or {}
        obra_ponto = (do_ponto.get("obra") or "").upper()
        obra_cadastro = (colaboradores.resolver_obra(ficha, por_nome) or "").upper()
        escolha = str((escolhidas.get(cpf) or {}).get("obra") or "")
        dividida = rateio_da_escolha(escolha)
        if dividida:
            obras_pct, de_onde_obra = dividida, "dividida"
        elif escolha:
            obras_pct, de_onde_obra = [{"obra": escolha, "percentual": Decimal("100")}], "mao"
        elif regras.get(cpf):
            obras_pct = regras[cpf].get("obras") or []
            de_onde_obra = "regra"
        elif obra_ponto:
            obras_pct, de_onde_obra = [{"obra": obra_ponto, "percentual": Decimal("100")}], "ponto"
        elif obra_cadastro:
            obras_pct, de_onde_obra = ([{"obra": obra_cadastro, "percentual": Decimal("100")}],
                                       "cadastro")
        else:
            obras_pct, de_onde_obra = [], ""
        rateio = (sorted(folha_rateio.distribuir(max(valor, Decimal("0.00")), obras_pct),
                         key=lambda x: -x["percentual"]) if obras_pct else [])
        for parte in rateio:
            parte["obra"] = " ".join(str(parte["obra"]).split()).upper()
            parte["conta"] = contas.get(parte["obra"], "")
            parte["omie"] = omie.get(folha_cards._chave(parte["obra"]), "")
        obra = rateio[0]["obra"] if rateio else ""

        p = {
            "cpf": cpf, "cpf_bonito": cpf_bonito(cpf), "nome": ficha.get("nome") or "",
            "cargo": ficha.get("cargo") or "",
            "tipo_contrato": ficha.get("tipo_contrato") or ficha.get("tipo") or "",
            "fase": ficha.get("fase") or "", "situacao": ficha.get("situacao") or "",
            "link_pipefy": ficha.get("link_pipefy") or "",
            "valor_gratificacao": _dinheiro(ficha.get("valor_gratificacao")),
            "valor_mensal": _dinheiro(docs.get("valor_mensal")),
            "valor_mes": base, "de_onde_valor": de_onde_valor,
            "parcela_unica": unica, "valor_quinzena": q, "valor_fim": f,
            "do_periodo": do_periodo, "ajuste": ajuste,
            "ajuste_motivo": aj.get("motivo") or "", "tem_ajuste": aj.get("ajuste") not in (None, ""),
            "valor": valor if valor > 0 else Decimal("0.00"),
            "recebe_no_periodo": do_periodo > 0,
            "obra": obra, "obra_de_onde": de_onde_obra,
            "rotulo_obra": ORIGEM_DA_OBRA.get(de_onde_obra, ""),
            "regra": (regras.get(cpf) or {}).get("nome") or "" if de_onde_obra == "regra" else "",
            "obra_ponto": obra_ponto, "dias_na_obra": do_ponto.get("dias") or 0,
            "obra_cadastro": obra_cadastro, "obra_escolhida": escolha,
            "rateio": rateio, "obras": [x["obra"] for x in rateio],
            "conta": rateio[0]["conta"] if rateio else "",
            "omie": rateio[0]["omie"] if rateio else "",
            "favorecido": _favorecido(ficha, docs) if via == SP else None,
            "gerada": cpf in gerados, "motivos": [], "impossivel": False,
        }
        if p["recebe_no_periodo"] or p["tem_ajuste"]:
            p["no_periodo"] = "recebe"
        elif base > 0:
            p["no_periodo"] = "so_fim"
        else:
            p["no_periodo"] = "sem_valor"
        m = p["motivos"]
        if base <= 0:
            p["impossivel"] = True
            m.append("sem Valor da Gratificação nem Valor do Mês no cadastro.")
        if alerta_valor:
            m.append(alerta_valor)
        if valor < 0:
            m.append(f"o desconto (R$ {formatos.moeda(ajuste)}) é maior que o valor do "
                     "período — nada a pagar.")
        if periodo == "quinzena" and unica and base > 0:
            m.append("parcela única: recebe o valor inteiro no fim do mês.")
        if ficha.get("situacao") == colaboradores.SITUACAO_SAIU:
            m.append(ficha.get("motivo") or "colaborador desligado no cadastro.")
        elif ficha.get("situacao") == colaboradores.SITUACAO_SAINDO:
            m.append(ficha.get("motivo") or "colaborador em desligamento.")
        elif ficha.get("situacao") == colaboradores.SITUACAO_AFASTADO:
            m.append("colaborador afastado no cadastro.")
        if ficha.get("desacordo"):
            m.append(ficha["desacordo"])
        if not rateio:
            m.append("sem obra: escolha a obra na linha (sem ela não há conta de pagamento).")
        else:
            for parte in rateio:
                if not parte["conta"]:
                    m.append(f'a obra {parte["obra"]} não tem conta de pagamento na aba '
                             '"C. Diários".')
                elif not parte["omie"]:
                    m.append(f'a obra {parte["obra"]} não tem Código Omie na aba '
                             '"C. Diários".')
        if de_onde_obra == "cadastro":
            m.append("obra do cadastro (sem ponto nem rateio) — confira.")
        if via == SP and p["favorecido"]["chave_do_cpf"]:
            m.append("sem chave Pix no cadastro: a SP vai com o CPF como chave.")
        if p["gerada"]:
            m.insert(0, "já gerado neste período — veja em Arquivos gerados.")
        sem_conta = any(not x["conta"] for x in rateio) or not rateio
        desligado = ficha.get("situacao") == colaboradores.SITUACAO_SAIU
        p["sem_obra"] = not rateio
        p["pagar_calculado"] = (not p["impossivel"] and p["valor"] > 0 and not sem_conta
                                and not desligado and not p["gerada"])
        p["pagar"] = (p["pagar_calculado"] if aj.get("pagar") is None
                      else bool(aj["pagar"]) and not p["impossivel"] and p["valor"] > 0)
        p["ajuste_pagar"] = aj.get("pagar")
        pessoas.append(p)

    pessoas.sort(key=lambda p: (p["pagar"], not p["motivos"], (p["nome"] or "").lower()))
    a_pagar = pessoas_a_pagar({"pessoas": pessoas})
    return {
        "via": via, "rotulo": ROTULO_DA_VIA[via], "verba": verba,
        "ano": ano, "mes": mes, "periodo": periodo,
        "rotulo_periodo": ROTULO_DO_PERIODO[periodo],
        "competencia": f"{mes:02d}/{ano}",
        "pessoas": pessoas, "quantos": len(pessoas), "quantos_a_pagar": len(a_pagar),
        "total": sum((p["valor"] for p in a_pagar), Decimal("0.00")),
        "total_do_mes": sum((p["valor_mes"] for p in pessoas), Decimal("0.00")),
        "total_quinzena": sum((p["valor_quinzena"] for p in pessoas), Decimal("0.00")),
        "total_fim": sum((p["valor_fim"] for p in pessoas), Decimal("0.00")),
        "total_ajustes": sum((p["ajuste"] for p in pessoas), Decimal("0.00")),
        "so_no_fim": [p for p in pessoas if p["no_periodo"] == "so_fim"],
        "com_problema": [p for p in pessoas if p["impossivel"] or (p["valor"] > 0 and not p["conta"])],
        "alertas": [p for p in pessoas if p["motivos"] and not p["impossivel"]],
        "na_outra_tela": len(da_outra),
        "gerados": gerados, "avisos": avisos,
        "resumos": {campo: resumo_por(a_pagar, campo) for campo in ("obra", "conta")},
        "pronto": True,
    }


def _gerados(ano, mes, periodo, verba) -> set:
    """Os CPFs que já saíram numa geração deste período (os lotes guardados)."""
    from . import folha_pagamento as fpg
    saida: set = set()
    try:
        for a in fpg.log(teto=400, ano=ano, mes=mes):
            if (a["destino"] == fpg.ANALISE and a["tipo"] == periodo
                    and verba in (a["verbas"] or "").split("+")):
                lote = lote_da_analise(a["id"]) or {}
                saida |= {l["cpf"] for l in lote.get("linhas") or []}
    except Exception:  # noqa: BLE001 — a tela abre sem a marca
        logger.exception("Gratificados: não consegui ler as gerações do período")
    return saida


def pessoas_a_pagar(calculado: dict) -> list:
    return [p for p in calculado["pessoas"]
            if p["pagar"] and p["valor"] > 0 and not p["gerada"]]


def resumo_por(pessoas, campo: str) -> list:
    """Total por obra ou por conta, pelas PARTES do rateio."""
    grupos: dict = {}
    for p in pessoas:
        for parte in p["rateio"] or [{"obra": "", "conta": "", "valor": p["valor"]}]:
            rotulo = parte.get(campo) or {"obra": "(sem obra)", "conta": "(sem conta)"}[campo]
            g = grupos.setdefault(rotulo, {"rotulo": rotulo, "cpfs": set(),
                                           "total": Decimal("0.00")})
            g["cpfs"].add(p["cpf"])
            g["total"] += parte["valor"]
    return [{"rotulo": g["rotulo"], "pessoas": len(g["cpfs"]), "total": g["total"]}
            for g in sorted(grupos.values(), key=lambda g: (-g["total"], g["rotulo"]))]


AGRUPAMENTOS = [("obra", "Obra"), ("conta", "Conta"), ("tipo_contrato", "Tipo de contrato"),
                ("", "Sem agrupar")]
AGRUPAMENTO_PADRAO = ""


def agrupar(pessoas, campo: str) -> list:
    from . import folha_lista
    return folha_lista.agrupar(
        pessoas, campo,
        rotulo=lambda p, c: p.get(c) or {"obra": "(sem obra)", "conta": "(sem conta)",
                                         "tipo_contrato": "(sem tipo)"}.get(c, "—"),
        pendente=lambda p: p["impossivel"] or (p["valor"] > 0 and not p["conta"]))


def linhas_a_pagar(calculado: dict) -> list:
    """Uma linha por pessoa E obra (as partes do rateio), no formato da geração."""
    verba = calculado["verba"]
    saida = []
    for p in pessoas_a_pagar(calculado):
        for parte in p["rateio"]:
            if parte["valor"] <= 0:
                continue
            saida.append({"cpf": p["cpf"], "nome": p["nome"], "obra": parte["obra"],
                          "conta": parte["conta"], "verba": verba, "valor": parte["valor"],
                          "dias": 0, "origem": p["obra_de_onde"] or verba,
                          "natureza": TIPO_DE_DESPESA,
                          "carteira": "Gratiticações e Extras"})
    return saida


# ---------------------------------------------------------------------------
# O LOTE — o que cada geração levou (é o que a SP do Pipefy lê)
# ---------------------------------------------------------------------------
def registrar_lote(analise_id, calculado: dict, quem: str = "") -> None:
    from .horario import agora
    pessoas = pessoas_a_pagar(calculado)
    lote = {"analise_id": analise_id, "via": calculado["via"], "ano": calculado["ano"],
            "mes": calculado["mes"], "periodo": calculado["periodo"],
            "por": str(quem or "")[:80], "em": agora().isoformat(),
            "linhas": [{"cpf": l["cpf"], "nome": l["nome"], "obra": l["obra"],
                        "conta": l["conta"], "valor": str(l["valor"])}
                       for l in linhas_a_pagar(calculado)],
            "pessoas": [{"cpf": p["cpf"], "nome": p["nome"],
                         "tipo_contrato": p["tipo_contrato"], "valor": str(p["valor"]),
                         "valor_mes": str(p["valor_mes"]),
                         "do_periodo": str(p["do_periodo"]), "ajuste": str(p["ajuste"]),
                         "ajuste_motivo": p["ajuste_motivo"],
                         "conta": p["conta"], "obra_de_onde": p["rotulo_obra"],
                         "favorecido": p["favorecido"],
                         "rateio": [{"obra": x["obra"], "conta": x["conta"],
                                     "omie": x["omie"], "valor": str(x["valor"]),
                                     "percentual": str(x["percentual"])}
                                    for x in p["rateio"]]}
                        for p in pessoas]}
    _meta_gravar(CHAVE_DO_LOTE.format(int(analise_id)), lote)


def lote_da_analise(analise_id: int) -> dict | None:
    lote = _meta_ler(CHAVE_DO_LOTE.format(int(analise_id)))
    if not lote:
        return None
    for l in lote.get("linhas") or []:
        l["valor"] = Decimal(str(l.get("valor") or 0))
    for p in lote.get("pessoas") or []:
        for campo in ("valor", "valor_mes", "do_periodo", "ajuste"):
            p[campo] = Decimal(str(p.get(campo) or 0))
        for x in p.get("rateio") or []:
            x["valor"] = Decimal(str(x.get("valor") or 0))
    return lote


def apagar_lotes(analise_ids) -> int:
    """Excluir a geração em Arquivos gerados devolve as pessoas para gerar de novo."""
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
    return n


# ---------------------------------------------------------------------------
# A GERAÇÃO DO PAGAMENTO VIA SP — sem arquivo de banco
#
# Não há BeeVale nem SomaPay: quem paga é o financeiro, SP a SP. O que fica no
# Drive é a RELAÇÃO de cada conta (pessoa, credor, Pix, valor e rateio), o
# relatório em PDF e a análise — e é dessa geração, em Arquivos gerados, que se
# lançam as SPs, uma por pessoa.
# ---------------------------------------------------------------------------
DESTINO_SP = "viasp"


def relacao_xlsx(pessoas) -> bytes:
    """A relação dos pagamentos via SP de uma conta."""
    import io
    from openpyxl import Workbook
    livro = Workbook()
    aba = livro.active
    aba.title = "Pagamentos via SP"
    aba.append(["Colaborador", "CPF", "Tipo de contrato", "Credor", "CPF/CNPJ do credor",
                "Tipo da chave Pix", "Chave Pix", "Valor do mês", "Valor do período",
                "Desconto/acréscimo", "Motivo do ajuste", "Valor a pagar", "Conta",
                "Rateio por obra"])
    for p in pessoas:
        fav = p.get("favorecido") or {}
        aba.append([p["nome"], cpf_bonito(p["cpf"]), p.get("tipo_contrato") or "",
                    fav.get("nome") or "", fav.get("documento") or "",
                    fav.get("tipo_chave") or "", fav.get("chave_pix") or "",
                    float(p["valor_mes"]), float(p["do_periodo"]), float(p["ajuste"]),
                    p.get("ajuste_motivo") or "", float(p["valor"]), p.get("conta") or "",
                    "; ".join(f"{x['obra']} {formatos.moeda(x['valor'])}"
                              for x in p.get("rateio") or [])])
    for coluna in aba.columns:
        largura = max(len(str(c.value or "")) for c in coluna)
        aba.column_dimensions[coluna[0].column_letter].width = min(60, max(10, largura + 2))
    aba.freeze_panes = "A2"
    memoria = io.BytesIO()
    livro.save(memoria)
    return memoria.getvalue()


def lotes_via_sp(calculado: dict) -> list:
    """Os "lotes" da geração via SP, um por conta, no formato da análise."""
    verba = calculado["verba"]
    por_conta: dict = {}
    for p in pessoas_a_pagar(calculado):
        lote = por_conta.setdefault(p["conta"], {
            "conta": p["conta"], "destino": DESTINO_SP, "verbas": [verba],
            "linhas": [], "pessoas": [], "criticas": []})
        lote["pessoas"].append(p)
        lote["linhas"].append({"cpf": p["cpf"], "nome": p["nome"], "verba": verba,
                               "natureza": TIPO_DE_DESPESA, "carteira": "",
                               "obra": p["obra"], "valor": p["valor"], "juntou": 1})
    lotes = []
    for conta, lote in sorted(por_conta.items()):
        if not conta:
            lote["criticas"].append("pessoas sem conta de pagamento — escolha a obra.")
        lote["quantos"] = len(lote["pessoas"])
        lote["total"] = sum((p["valor"] for p in lote["pessoas"]), Decimal("0.00"))
        lote["pode_gerar"] = not lote["criticas"]
        lotes.append(lote)
    return lotes


def gerar_via_sp(calculado: dict, quem: str = "", forcar: bool = False) -> dict:
    """Grava a relação de cada conta, o relatório e a análise no Drive e no
    registro, e guarda o lote. NÃO cria SP: isso é em Arquivos gerados."""
    from . import drive, folha_geracao as geracao, folha_pagamento as fpg
    from . import folha_relatorio as fr
    from .beevale import pasta_do_drive

    lotes = lotes_via_sp(calculado)
    if not lotes:
        raise ErroDosGratificados("ninguém selecionado para pagamento neste período.")
    if any(not l["pode_gerar"] for l in lotes) and not forcar:
        raise ErroDosGratificados("há avisos na geração. Confira e marque \"gerar com "
                                  "aviso\", se for o caso.")
    pasta, _ = pasta_do_drive()
    if not pasta:
        raise ErroDosGratificados("pasta do Drive não configurada. Em Configurações, "
                                  "informe o identificador da pasta de destino.")
    ano, mes, periodo, verba = (calculado["ano"], calculado["mes"], calculado["periodo"],
                                calculado["verba"])
    gerados = []
    for lote in lotes:
        nome = geracao.nome_do_arquivo(lote, ano, mes, periodo)
        subido = drive.subir_arquivo(relacao_xlsx(lote["pessoas"]), nome, pasta)
        gerados.append(fpg._registrar(ano, mes, periodo, DESTINO_SP, verba,
                                      lote["conta"], nome, lote["quantos"], lote["total"],
                                      subido, "; ".join(lote["criticas"]), quem))
    contas = fpg.conta_por_obra()
    montado = montado_do_relatorio(calculado, so_a_pagar=True)
    for lote in lotes:
        try:
            dados = fr.montar(dict(montado, pessoas=[
                x for x in montado["pessoas"] if x["conta_sp"] == lote["conta"]]),
                contas, "")
            dados["titulo"] += f" — conta {lote['conta'] or 'sem conta'}"
            conteudo, nome_pdf = fr.pdf(dados), fr.nome_do_arquivo(dados, "pdf")
            nome_pdf = nome_pdf.replace(".pdf", f" - conta {lote['conta']}.pdf")
            subido = drive.subir_arquivo(conteudo, nome_pdf, pasta, mime=fpg.MIME_PDF)
            gerados.append(fpg._registrar(ano, mes, periodo, fpg.RELATORIO, verba,
                                          lote["conta"], nome_pdf, lote["quantos"],
                                          lote["total"], subido, "", quem))
        except Exception:  # noqa: BLE001 — a relação já está no Drive
            logger.exception("Via SP: não consegui gerar o relatório da conta %s",
                             lote["conta"])
    detalhe = linhas_a_pagar(calculado)
    analise = geracao.analise_xlsx(lotes, ano=ano, mes=mes, tipo=periodo, detalhe=detalhe)
    nome_analise = geracao.nome_do_arquivo(
        {"destino": fpg.ANALISE, "conta": "", "verbas": [verba]}, ano, mes, periodo)
    subido = drive.subir_arquivo(analise, nome_analise, pasta)
    total = sum((l["total"] for l in lotes), Decimal("0.00"))
    pessoas = sum(l["quantos"] for l in lotes)
    gerados.append(fpg._registrar(ano, mes, periodo, fpg.ANALISE, verba, "", nome_analise,
                                  pessoas, total, subido, "", quem))
    registrar_lote(gerados[-1]["id"], calculado, quem=quem)
    logger.info("Via SP: %d pessoa(s), R$ %s, %02d/%d %s, gerado por %s.", pessoas,
                total, mes, ano, periodo, quem or "?")
    return {"ok": True, "arquivos": gerados, "competencia": f"{mes:02d}/{ano}",
            "resumo": {"pessoas": pessoas, "total": total}}


# ---------------------------------------------------------------------------
# O RELATÓRIO — o mesmo das outras folhas, com o rateio de cada pessoa
# ---------------------------------------------------------------------------
def montado_do_relatorio(calculado: dict, pessoas=None, filtros=None,
                         so_a_pagar: bool = False) -> dict:
    pessoas = calculado["pessoas"] if pessoas is None else pessoas
    linhas = []
    for p in pessoas:
        entra = bool(p["pagar"] and p["valor"] > 0)
        if so_a_pagar and (not entra or p["gerada"]):
            continue
        ajuste = (f" · ajuste {formatos.moeda(p['ajuste'])} ({p['ajuste_motivo']})"
                  if p["tem_ajuste"] else "")
        linhas.append({
            "entra": entra, "cpf": p["cpf"], "nome_na_tela": p["nome"] or p["cpf_bonito"],
            "cargo": p["cargo"], "fase": p["fase"],
            "por_obra": [{"obra": x["obra"], "dias": 0, "valor": x["valor"]}
                         for x in p["rateio"]] or [{"obra": "", "dias": 0, "valor": p["valor"]}],
            "por_dia": [], "dias_no_ponto": 0, "valor": p["valor"], "valor_por_dia": None,
            "obras_resumo": " / ".join(f"{x['obra']} {formatos.moeda(x['percentual'])}%"
                                       for x in p["rateio"]) or "-",
            "obra_do_cadastro": p["obra_cadastro"], "conta_sp": p["conta"],
            "contas": sorted({x["conta"] for x in p["rateio"] if x["conta"]}),
            "situacao_rotulo": (f"{p['tipo_contrato'] or '—'} · mês "
                                f"{formatos.moeda(p['valor_mes'])}"
                                + (" · parcela única" if p["parcela_unica"] else "")
                                + ajuste)})
    rotulo = calculado["rotulo"]
    return {"titulo": f"{rotulo} {calculado['competencia']}",
            "prefixo_arquivo": rotulo,
            "folha": {"competencia": calculado["competencia"],
                      "rotulo_do_tipo": calculado["rotulo_periodo"]},
            "pessoas": linhas, "filtros_texto": [], "filtros": filtros or {},
            "totais": {"pessoas": len(pessoas)}, "fechamento": None}
