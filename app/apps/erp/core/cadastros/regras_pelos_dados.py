# -*- coding: utf-8 -*-
"""
AS CONDICIONAIS DESCOBERTAS NOS CADASTROS — porque o Pipefy não as exporta.

⚑ POR QUE ESTE ARQUIVO EXISTE (17/11/2026).

Ele pediu ajuda de verdade na construção, e explicou o impasse:

> *"O pipe não exporta condicionais, por isso minha pergunta sobre mais ajuda
> na construção e na ideia."*

E antes disso, o problema que isso causa:

> *"Eu estou tendo que corrigir minúcia a minúcia, porque nada está ficando
> 100%. E está muito desgastante, cansativo e demorado."*

**O impasse:** a exportação do Pipefy (04/11/2026) trouxe os campos e as fases,
mas **as condicionais — "este campo só aparece se aquele for tal" e "este campo
é obrigatório quando aquele for tal" — não saem de lá.** Elas existem na
configuração do Pipe, que não se exporta, e na cabeça dele. Perguntar uma por
uma é exatamente o desgaste que ele descreveu: são dezenas.

**A ideia que resolve sem depender da memória dele: a regra deixa RASTRO NO
DADO.** Se no Pipefy o PIS era obrigatório para quem tem carteira, então entre
os CTPS do cadastro o PIS está preenchido em quase 100% dos casos; e se o campo
nem aparecia para prestador, entre os prestadores ele está vazio em 100%. A
regra não precisa ser lembrada — ela pode ser **medida** nos 2.600 cadastros
que a importação trouxe.

**O que muda no método, e é o ponto todo:** em vez de ele ditar a regra, ele
CONFERE a regra proposta com o número do lado e a exceção nomeada. Ler *"nos
1.843 CTPS o PIS está preenchido em 1.840 — proponho obrigatório; os 3 sem são
estes"* é uma passada. Ditar quarenta condicionais é uma tarde.

⚠️ **ISTO NÃO DECIDE NADA SOZINHO.** O módulo só MEDE e PROPÕE. Nada aqui
muda formulário, e a adoção de cada regra é ação dele — pelo mesmo motivo do
agente de DP: regra de cadastro errada custa dinheiro na folha, e o dado da
importação pode estar errado justamente por falta da regra.

⚠️ **E ELE PODE MEDIR O CONTRÁRIO DO QUE SE QUER.** Se um campo foi mal
preenchido por anos, a medição vai dizer "quase sempre vazio" e propor esconder
— quando o certo seria cobrar. Por isso toda proposta vem com **quantos casos a
sustentam e quantos a contradizem**, e as exceções vêm nomeadas: é olhando os
três discordantes que se descobre se são erro de cadastro ou se a regra é outra.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# O QUE SE CRUZA COM O QUÊ
#
# Os campos-chave são os que, no formulário, mandam nos outros — e são poucos
# de propósito: vínculo e tipo de contrato são as duas perguntas que mudam o
# resto do cadastro. Cruzar tudo com tudo daria centenas de pares sem sentido
# (nascimento × agência) e afogaria o que importa.
# ---------------------------------------------------------------------------
CHAVES = (
    ("vinculo", "Vínculo"),
    ("tipo_contrato", "Tipo de contrato"),
)

# Os campos que podem ou não se aplicar conforme a chave. Fora daqui ficam as
# colunas técnicas e as que valem para todo mundo (nome, CPF, telefone).
DEPENDENTES = (
    ("pis", "Nº do PIS"),
    ("ctps_numero", "Nº da CTPS"),
    ("ctps_serie", "Série da CTPS"),
    ("salario", "Salário"),
    ("valor_diaria", "Valor da diária"),
    ("valor_mes", "Valor do mês"),
    ("razao_social_pj", "Razão social (PJ)"),
    ("cnpj_pj", "CNPJ (PJ)"),
    ("categoria_alimentacao", "Ajuda com alimentação"),
    ("aux_alimentacao", "Valor da alimentação"),
    ("categoria_transporte", "Ajuda com transporte"),
    ("aux_transporte", "Valor do transporte"),
    ("convencao_id", "Convenção"),
    ("cargo_id", "Cargo"),
    ("data_inicio", "Data de início"),
    ("admissao", "Data de admissão"),
    ("contrato_experiencia", "Contrato de experiência"),
    ("empresa_id", "Empresa de registro"),
)

# ⚠️ ABAIXO DE 20 CASOS NÃO SE PROPÕE REGRA. Com 3 pessoas, "100% preenchido"
# não é regra: é coincidência. O número é conservador de propósito — regra
# proposta por engano gasta a confiança dele, e ele já disse que está cansado.
MINIMO_DE_CASOS = 20
# Acima disto a tela propõe OBRIGATÓRIO; abaixo do espelho, propõe ESCONDER.
CORTE_SEMPRE = 0.97
CORTE_NUNCA = 0.03
# Quantas exceções mostrar por regra: o suficiente para ele olhar uma por uma.
TETO_DE_EXCECOES = 8


# ===========================================================================
# ⚑ O QUE ELE JÁ DECIDIU — para a tela não perguntar duas vezes (18/11/2026)
#
# Ele revisou a medição dos 660 cadastros e recusou SEIS propostas. Sem
# registrar isso, a próxima medição ofereceria as mesmas seis de novo — e
# perguntar de novo o que ele já respondeu é a mesma irritação da pausa no meio
# da fila, que o `CLAUDE.md` descreve.
#
# ⚠️ O MOTIVO DELE IMPORTA MAIS QUE O "NÃO", e é por isso que ele está escrito:
# nos dois casos o número estava certo e a conclusão errada, pela mesma razão —
# **o campo é raro, não é inaplicável.** Mensalista de carteira existe (4 em
# 413) e prestador com ajuda de alimentação existe (3 em 222). Esconder o campo
# tiraria da tela a única forma de registrar esses casos, e eles voltariam a
# ser resolvidos por fora do sistema.
#
# A lição para a medição: **"quase nunca" não é "nunca".** O corte de 97% acha
# o campo que não se aplica, mas não distingue dele o campo que se aplica
# pouco. Só quem conhece o negócio distingue — foi o que ele fez aqui.
# ===========================================================================
RECUSADAS: dict[tuple[str, str, str], str] = {
    ("vinculo", "CTPS", "valor_mes"):
        "mensalista de carteira existe (4 em 413) — o campo é raro, não "
        "inaplicável; esconder tiraria o único jeito de registrar esses casos",
    ("tipo_contrato", "CLT_DETERMINADO", "valor_mes"):
        "o mesmo caso do vínculo CTPS: mensalista de carteira existe",
    ("vinculo", "PRESTADOR", "aux_alimentacao"):
        "prestador com ajuda de alimentação existe (1 em 222) — raro, não "
        "inaplicável",
    ("vinculo", "PRESTADOR", "categoria_alimentacao"):
        "idem: 3 prestadores têm a categoria preenchida",
    ("tipo_contrato", "AUTONOMO_RPA", "aux_alimentacao"):
        "idem para o autônomo RPA (1 em 207)",
    ("tipo_contrato", "AUTONOMO_RPA", "categoria_alimentacao"):
        "idem para o autônomo RPA (3 em 207)",
    # ⚠️ ESTAS DUAS NÃO FORAM RECUSADAS AGORA — elas aplicam uma decisão que
    # ele JÁ tomou em 10/11/2026: *"quem tem carteira pode ser pago por dia"*.
    # Esconder a diária de quem é CTPS contrariaria aquilo, e a medição não tem
    # como saber: a diária está vazia em 100% dos CTPS da amostra porque é raro,
    # não porque é proibido — o mesmo caso do "valor do mês".
    #
    # Registradas aqui para a tela não ficar oferecendo o oposto de uma decisão
    # dele. Se eu li errado, é tirar estas duas linhas.
    ("vinculo", "CTPS", "valor_diaria"):
        "ele decidiu em 10/11/2026 que quem tem carteira PODE ser pago por dia "
        "— esconder a diária do CTPS contrariaria isso",
    ("tipo_contrato", "CLT_DETERMINADO", "valor_diaria"):
        "o mesmo caso: CLT pago por dia existe, por decisão dele de 10/11/2026",
    ("tipo_contrato", "CLT_INDETERMINADO", "valor_diaria"):
        "o mesmo caso: CLT pago por dia existe, por decisão dele de 10/11/2026",
    # ⚑ 19/11/2026, decisão dele: *"não tornar item obrigatório, porque o
    # cadastro social é feito com o CPF."* A medição não volta a propor.
    ("vinculo", "CTPS", "pis"):
        "o PIS não é obrigatório: o eSocial é feito com o CPF (decisão dele, "
        "19/11/2026)",
    ("tipo_contrato", "CLT_INDETERMINADO", "pis"):
        "idem: PIS não é obrigatório",
    ("tipo_contrato", "CLT_DETERMINADO", "pis"):
        "idem: PIS não é obrigatório",
    # ⚑ 20/11/2026, ele: *"Autônomo não tem convenção. Apenas se define o
    # valor da diária."* Os 207 de 207 com convenção preenchida são sobra do
    # Pipefy — o número estava certo e a conclusão errada.
    ("tipo_contrato", "AUTONOMO_RPA", "convencao_id"):
        "autônomo não tem convenção — só o valor da diária (decisão dele, "
        "20/11/2026); a convenção que aparece nos cadastros é sobra do Pipefy",
    ("vinculo", "PRESTADOR", "convencao_id"):
        "prestador não tem convenção coletiva (decisão dele, 20/11/2026)",
}


def _recusada(p: dict[str, Any]) -> Optional[str]:
    return RECUSADAS.get((p["chave"], p["valor"], p["campo"]))


def _vazio(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip()
    return False


def medir(s: Session, *, minimo: int = MINIMO_DE_CASOS) -> dict[str, Any]:
    """Mede, nos cadastros, o rastro que cada condicional deixou.

    Devolve as regras PROPOSTAS, cada uma com o número que a sustenta, o que a
    contradiz e as exceções nomeadas. Só leitura — não muda nada.
    """
    from app.apps.erp.core.cadastros import pessoas as svc

    colunas = ", ".join(dict.fromkeys(
        ["id", "nome", "cpf"] + [c for c, _ in CHAVES]
        + [c for c, _ in DEPENDENTES]))
    linhas = s.execute(text(
        f"SELECT {colunas} FROM colaboradores "
        # Cadastro cancelado nunca virou gente na empresa: incluí-lo mediria o
        # preenchimento de ficha abandonada, não a regra do cadastro.
        f"WHERE UPPER(COALESCE(etapa, 'CONCLUIDO')) <> 'CANCELADO'"
    )).mappings().all()

    rotulos = dict(DEPENDENTES)
    rotulos_chave = dict(CHAVES)
    # Quem voltou de carteira a carteira em até 1 ano (sem experiência).
    readmitidos: set[int] = set(s.execute(text("""
        SELECT a.colaborador_id FROM colaborador_vinculos a
          JOIN colaborador_vinculos b ON b.colaborador_id = a.colaborador_id
                                     AND b.demissao IS NOT NULL
                                     AND b.demissao < a.admissao
         WHERE a.demissao IS NULL
           AND UPPER(COALESCE(a.vinculo, 'CTPS')) = 'CTPS'
           AND UPPER(COALESCE(b.vinculo, 'CTPS')) = 'CTPS'
           AND a.admissao - b.demissao <= :d"""),
        {"d": svc.DIAS_READMISSAO_SEM_EXPERIENCIA}).scalars().all())
    propostas: list[dict[str, Any]] = []
    ignoradas: list[dict[str, Any]] = []
    # Campo vazio em todo mundo: achado próprio, contado uma vez por campo.
    vazios: list[dict[str, Any]] = []
    _ja_contado_vazio: set[str] = set()

    for chave, _ in CHAVES:
        # agrupa por valor da chave (vazio fora: não é um caso, é falta de dado)
        grupos: dict[str, list] = {}
        for l in linhas:
            v = l[chave]
            if _vazio(v):
                continue
            grupos.setdefault(str(v).strip().upper(), []).append(l)

        for valor, gente in sorted(grupos.items()):
            for campo, _ in DEPENDENTES:
                if campo == chave:
                    continue
                preenchidos = [p for p in gente if not _vazio(p[campo])]
                total, tem = len(gente), len(preenchidos)
                proporcao = tem / total if total else 0.0
                base = {
                    "chave": chave, "chave_rotulo": rotulos_chave[chave],
                    "valor": valor,
                    "valor_rotulo": _rotulo_do_valor(svc, chave, valor),
                    "campo": campo, "campo_rotulo": rotulos[campo],
                    "casos": total, "preenchidos": tem,
                    "proporcao": round(proporcao, 4),
                }
                if total < minimo:
                    ignoradas.append({**base, "porque":
                                      f"só {total} caso(s) — abaixo de {minimo}"})
                    continue
                # ===========================================================
                # ⚑ O CONTRASTE — e sem ele o módulo diz bobagem
                #
                # Achado dirigindo a tela, na primeira medição: ela propôs
                # *"não exibir Data de admissão para CLT"*, porque na amostra
                # o campo estava vazio nos 40. É absurdo, e o número não tinha
                # como saber.
                #
                # A causa é que "vazio em 100%" tem DOIS significados opostos:
                #   (a) o campo não se aplica a este caso — é condicional;
                #   (b) ninguém preenche esse campo em caso NENHUM — é dado
                #       faltando em todo mundo, e esconder seria encobrir.
                #
                # O que separa os dois é olhar o RESTO: se o campo é preenchido
                # para os OUTROS valores da chave, há contraste e é condicional
                # de verdade. Se está vazio para todos, não é condicional — é
                # um campo que ninguém usa, e isso é outro achado (útil também,
                # com outro nome).
                #
                # Vale para os dois lados: campo preenchido em 100% de TODO
                # MUNDO não é condicional, é obrigatório sempre — e dizer
                # "obrigatório QUANDO CTPS" sobre ele seria ensinar errado.
                # ===========================================================
                fora = [p for p in linhas
                        if not _vazio(p[chave])
                        and str(p[chave]).strip().upper() != valor]
                fora_preenchidos = sum(1 for p in fora if not _vazio(p[campo]))
                prop_fora = (fora_preenchidos / len(fora)) if fora else None
                base["casos_fora"] = len(fora)
                base["preenchidos_fora"] = fora_preenchidos

                if proporcao >= CORTE_SEMPRE:
                    if prop_fora is not None and prop_fora >= CORTE_SEMPRE:
                        base["proposta"] = "OBRIGATORIO_SEMPRE"
                        base["frase"] = (
                            f"“{rotulos[campo]}” está preenchido em quase todos "
                            f"os cadastros, com {rotulos_chave[chave]} ou sem — "
                            f"proponho OBRIGATÓRIO PARA TODOS, não condicional.")
                        base["excecoes"] = _nomes(
                            [p for p in gente if _vazio(p[campo])])
                        base["o_que_sao_as_excecoes"] = (
                            "estes estão sem o campo e deveriam ter")
                        propostas.append(base)
                        continue
                    base["proposta"] = "OBRIGATORIO"
                    base["frase"] = (
                        f"Quando {rotulos_chave[chave]} é "
                        f"“{base['valor_rotulo']}”, “{rotulos[campo]}” está "
                        f"preenchido em {tem} de {total} cadastros "
                        f"({proporcao:.1%}) — proponho OBRIGATÓRIO.")
                    sem = [p for p in gente if _vazio(p[campo])]
                    # ⚑ A READMISSÃO EM ATÉ 1 ANO NÃO TEM EXPERIÊNCIA (regra
                    # dele, 20/11/2026): quem voltou nesse prazo está CERTO
                    # sem o campo, e não é discordante.
                    if campo == "contrato_experiencia":
                        certos = [p for p in sem if p["id"] in readmitidos]
                        sem = [p for p in sem if p["id"] not in readmitidos]
                        if certos:
                            base["frase"] += (
                                f" ({len(certos)} sem o campo voltaram em até "
                                "1 ano — readmissão entra sem experiência, "
                                "e está certo.)")
                    base["excecoes"] = _nomes(sem)
                    base["o_que_sao_as_excecoes"] = (
                        "estes estão sem o campo: ou a regra tem exceção, ou o "
                        "cadastro deles está incompleto")
                    propostas.append(base)
                elif proporcao <= CORTE_NUNCA:
                    if prop_fora is None or prop_fora <= CORTE_NUNCA:
                        # ⚠️ SEM CONTRASTE NÃO É CONDICIONAL. O campo está
                        # vazio aqui E lá fora: esconder seria encobrir falta
                        # de dado. Isto entra UMA vez por campo, não uma por
                        # valor da chave, senão enche a tela com a mesma notícia.
                        # ⚑ CAMPO QUE ELE JÁ DECIDIU SER CONDICIONAL não é
                        # "campo que ninguém preenche" (20/11/2026: *"CNPJ PJ
                        # e Razão Social PJ são obrigatórios para o Tipo de
                        # Contrato PJ"*). São poucos PJ — abaixo do mínimo da
                        # medição —, e por isso a medição não via o contraste.
                        if "tipo_contrato" in (svc.CAMPOS_SO_SE.get(campo) or {}):
                            continue
                        if campo not in _ja_contado_vazio:
                            _ja_contado_vazio.add(campo)
                            vazios.append({
                                "campo": campo, "campo_rotulo": rotulos[campo],
                                "casos": len(linhas),
                                "preenchidos": sum(1 for p in linhas
                                                   if not _vazio(p[campo])),
                                "frase": (
                                    f"“{rotulos[campo]}” está vazio em quase "
                                    f"todos os cadastros. Não é condicional: é "
                                    f"campo que ninguém preenche. Ou ele não "
                                    f"serve para nada, ou falta dado em todo "
                                    f"mundo — e esconder encobriria isso."),
                            })
                        continue
                    base["proposta"] = "NAO_SE_APLICA"
                    base["frase"] = (
                        f"Quando {rotulos_chave[chave]} é "
                        f"“{base['valor_rotulo']}”, “{rotulos[campo]}” está "
                        f"vazio em {total - tem} de {total} cadastros "
                        f"({1 - proporcao:.1%}) — proponho NÃO EXIBIR o campo.")
                    base["excecoes"] = _nomes(preenchidos)
                    base["o_que_sao_as_excecoes"] = (
                        "estes TÊM o campo preenchido: ou o campo faz sentido "
                        "em algum caso, ou foi preenchido por engano")
                    propostas.append(base)

    propostas.sort(key=lambda p: (-p["casos"], p["chave"], p["valor"], p["campo"]))
    logger.info("ERP/regras: %s cadastros medidos, %s regras propostas",
                len(linhas), len(propostas))
    return {
        "cadastros_medidos": len(linhas),
        "minimo_de_casos": minimo,
        "propostas": propostas,
        "campos_que_ninguem_preenche": sorted(
            vazios, key=lambda v: v["campo_rotulo"]),
        "sem_base": len(ignoradas),
        "obrigatorios": sum(1 for p in propostas if p["proposta"] == "OBRIGATORIO"),
        "nao_se_aplicam": sum(1 for p in propostas if p["proposta"] == "NAO_SE_APLICA"),
        "obrigatorios_sempre": sum(1 for p in propostas
                                   if p["proposta"] == "OBRIGATORIO_SEMPRE"),
    }


def _rotulo_do_valor(svc, chave: str, valor: str) -> str:
    """O nome que a pessoa lê, não o código gravado."""
    if chave == "vinculo":
        return svc.VINCULO_ROTULOS.get(valor, valor)
    if chave == "tipo_contrato":
        return svc.TIPO_CONTRATO_ROTULOS.get(valor, valor)
    return valor


def _nomes(gente: list) -> list[dict[str, Any]]:
    """As exceções, nomeadas — é olhando uma por uma que ele decide."""
    return [{"id": int(p["id"]), "nome": p["nome"], "cpf": p["cpf"]}
            for p in gente[:TETO_DE_EXCECOES]]


def comparar_com_o_formulario(s: Session, medido: Optional[dict] = None
                              ) -> dict[str, Any]:
    """O que o cadastro JÁ EXIGE × o que os dados dizem.

    ⚠️ É esta função que transforma a medição em trabalho: ela separa o que já
    está no formulário (nada a fazer) do que os dados pedem e o formulário não
    faz (a lista dele). Sem isso, a medição devolveria dezenas de regras que já
    valem, e a parte útil se perderia no meio.
    """
    from app.apps.erp.core import pessoal_solicitacoes as svc_sol
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas

    medido = medido or medir(s)

    # ===================================================================
    # ⚑ AS TRÊS FONTES, e a falta da primeira era o defeito (18/11/2026)
    #
    # Ele: *"quando eu coloco medir de novo, as regras anteriormente
    # definidas não deveriam sumir?"*
    #
    # Deveriam. Esta função olhava SÓ o formulário do portal — então as
    # regras que ele adotou e eu implementei na tabela do cadastro
    # (`OBRIGATORIOS_*`) e nas regras de visibilidade (`CAMPOS_SO_SE`)
    # continuavam aparecendo como "ainda não faz". A tela pedia de novo o
    # que ele já tinha respondido, que é a irritação que o CLAUDE.md
    # descreve.
    #
    # ⚠️ TRÊS LUGARES PODEM IMPLEMENTAR UMA REGRA, e a comparação tem de
    # conhecer os três: o que o cadastro EXIGE (a tabela), o que a tela
    # ESCONDE (a visibilidade) e o que o pedido do portal exige.
    # ===================================================================
    ja_exige: set[tuple[str, str]] = set()

    # 1. A TABELA DO CADASTRO — "o que falta para admitir"
    for campo, _ in svc_pessoas.OBRIGATORIOS_SEMPRE:
        ja_exige.add(("*", campo))
    for valor, campos in svc_pessoas.OBRIGATORIOS_POR_VINCULO.items():
        for campo, _ in campos:
            ja_exige.add((str(valor).upper(), campo))
    for valor, campos in svc_pessoas.OBRIGATORIOS_POR_CONTRATO.items():
        for campo, _ in campos:
            ja_exige.add((str(valor).upper(), campo))

    # 2. O PEDIDO DO PORTAL (os nomes de campo dele diferem de alguns da
    #    coluna — ver `_DE_PARA_FORMULARIO`)
    regra = svc_sol.TIPOS.get("CADASTRO", {})
    for campo in regra.get("obrigatorios", ()):
        ja_exige.add(("*", campo))
    for chave, por_valor in (regra.get("obrigatorios_se") or {}).items():
        for valor, campos in (por_valor or {}).items():
            for campo in campos:
                ja_exige.add((str(valor).upper(), campo))

    novas, confirmadas, recusadas = [], [], []
    for p in medido["propostas"]:
        porque = _recusada(p)
        if porque:
            recusadas.append({**p, "porque_recusada": porque})
            continue
        # O nome do campo no formulário do pedido não é sempre o da coluna.
        alvo = _campo_no_formulario(p["campo"])
        if p["proposta"] in ("OBRIGATORIO", "OBRIGATORIO_SEMPRE"):
            # ⚠️ A IMPLICAÇÃO VALE AQUI TAMBÉM, e sem ela a MESMA regra
            # aparecia duas vezes (18/11/2026, achado dirigindo a tela).
            #
            # A tabela declara "PIS é de quem tem CTPS" — por VÍNCULO. A
            # medição pergunta também por CONTRATO: "PIS para CLT tempo
            # determinado?". São a mesma coisa (CLT determinado é CTPS), mas a
            # comparação por chave não via, e ele veria cinco linhas pendentes
            # de decisões que ele já tomou.
            valores = {p["valor"]}
            if p["chave"] == "tipo_contrato":
                implicado = svc_pessoas.vinculo_pelo_contrato(p["valor"])
                if implicado:
                    valores.add(implicado)
            feita = any(
                (v, nome) in ja_exige or ("*", nome) in ja_exige
                for v in valores for nome in (alvo, p["campo"]))
        else:
            # 3. A VISIBILIDADE: "não exibir X quando a chave vale V" já está
            #    feito se a tela só mostra X para OUTROS valores. Pergunta-se
            #    ao próprio mecanismo da tela, para a resposta não divergir
            #    dele — e ele já trata a implicação contrato → vínculo.
            feita = not svc_pessoas.campo_aparece(
                p["campo"],
                vinculo=p["valor"] if p["chave"] == "vinculo" else "",
                tipo_contrato=p["valor"] if p["chave"] == "tipo_contrato" else "")
        (confirmadas if feita else novas).append(p)
    return {**medido, "ja_no_formulario": confirmadas, "a_decidir": novas,
            "recusadas_por_ele": recusadas}


# A coluna do cadastro × o nome do campo no formulário do pedido. Só os que
# diferem — o resto é igual, e inventar nome aqui seria o erro que ele cobrou
# em 10/11/2026 ("por que está querendo inventar a roda de novo?").
_DE_PARA_FORMULARIO = {
    "pis": "num_pis",
    "ctps_numero": "num_ctps",
    "salario": "valor_mes",
}


def _campo_no_formulario(coluna: str) -> str:
    return _DE_PARA_FORMULARIO.get(coluna, coluna)
