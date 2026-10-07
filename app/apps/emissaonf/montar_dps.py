# -*- coding: utf-8 -*-
"""
Ponte entre o cálculo fiscal que já existe e o formato da declaração nacional.

A prefeitura de Eusébio desativou o modelo antigo (ABRASF) em 07/10/2026, por
causa da obrigatoriedade do IBS/CBS, e passou a aceitar só a **DPS** (Declaração
de Prestação de Serviço) do padrão nacional. O que mudou foi o FORMATO do papel,
não a conta: as retenções continuam saindo de `tributacao.py`, e o corpo da nota
continua sendo a discriminação que a pessoa confere na tela.

Então este módulo não decide nada de fiscal. Ele traduz — e é de propósito que
ele seja só tradução: assim existe um lugar só onde o formato novo encosta no
resto, e a conta das retenções continua tendo um dono só.

Os três campos que, se errados, fazem a nota sair errada **sem ninguém
perceber** estão comentados um por um abaixo. Leia antes de mexer.
"""
from __future__ import annotations

import datetime
from decimal import Decimal

import el_nfse_nacional as nac

# Fuso de Brasília. O servidor roda em UTC, e a declaração exige a hora local
# com o deslocamento escrito — mandar Z faria a prefeitura ler 3 horas a mais.
FUSO_BRASILIA = datetime.timezone(datetime.timedelta(hours=-3))

# Teto do percentual de ISS que o layout aceita (XSD: TSDec1V2 = 1 dígito + 2
# decimais). Alíquota de 10% ou mais é recusada pelo schema, então é melhor
# falhar aqui, com o motivo escrito, do que levar erro da prefeitura.
ALIQUOTA_ISS_MAXIMA = Decimal("9.99")


class DadoIncompativel(ValueError):
    """Crítica: o dado da nota não cabe no layout nacional. Emissão barrada."""


def _v(valor) -> str:
    """Formata um valor no padrão do layout: sempre com duas casas decimais."""
    return f"{Decimal(str(valor or 0)):.2f}"


def numero_dps(numero_nota, ano: int) -> int:
    """Número da DPS a partir do número da nota e do ano.

    Mantém a convenção que o repositório já usa há meses (`job_nacional._id_dps`):
    nDPS = ano em 2 dígitos + o número da nota com 13 dígitos. Isso importa mais
    do que parece: a identificação da DPS é construída a partir daqui, e ela é a
    chave por onde a busca na SEFIN encontra a nota depois. Mudar o formato
    quebraria o reencontro de todas as notas antigas.
    """
    return int(f"{int(ano) % 100:02d}{int(numero_nota):013d}")


def montar(card: dict, obra, r, dados_rps, numero_nota, ibge_obra,
           data_emissao: str, producao: bool) -> nac.DadosDPS:
    """Traduz a nota já calculada para a declaração nacional.

    `numero_nota` é o número ESPERADO, usado para compor a identificação da
    declaração. O número de verdade da nota é o que a prefeitura devolve depois
    — e pode divergir.
    """
    ano = int(str(data_emissao)[:4])
    mes = int(str(data_emissao)[5:7])

    # --- 1. ISS retido: o campo cujo significado está invertido na intuição ---
    # No layout nacional, 1 é "NÃO retido" e 2 é "retido pelo tomador". As notas
    # da BWS são retidas na fonte. Mandar o número errado declara à prefeitura
    # que quem deve o ISS é a BWS, e não o tomador que já descontou.
    tp_ret_iss = nac.RET_ISS_TOMADOR if r.iss_retido else nac.RET_ISS_NAO_RETIDO

    # --- 2. Dedução de material: a causa do ISS a maior em setembro/2026 ---
    # A prefeitura calcula a base do ISS como (valor do serviço - dedução). Se a
    # dedução não for enviada, ela cobra sobre o valor cheio. É o mesmo cálculo
    # que o modelo antigo passou a mandar depois do incidente.
    deducao = Decimal(str(r.valor_total)) - Decimal(str(r.base_iss))
    if deducao < 0:
        deducao = Decimal("0")

    # --- 3. Quais federais foram retidos ---
    # O layout não tem um campo por imposto: tem um código que diz, de uma vez,
    # quais dos três (PIS, COFINS, CSLL) foram retidos. Errar o código declara
    # retenção que não houve, ou esconde a que houve.
    fed = r.federais_retidos or {}
    tem_pis, tem_cofins = "PIS" in fed, "COFINS" in fed
    tem_csll, tem_ir = "CSLL" in fed, "IR" in fed

    pis_cofins = None
    if tem_pis or tem_cofins:
        pis_cofins = {
            "CST": "01",                       # operação tributável, alíquota básica
            "vBCPisCofins": _v(r.valor_total),
            "pAliqPis": "0.65",
            "pAliqCofins": "3.00",
            "vPis": _v(r.pis if tem_pis else 0),
            "vCofins": _v(r.cofins if tem_cofins else 0),
            "tpRetPisCofins": nac.tipo_retencao_pis_cofins(tem_pis, tem_cofins, tem_csll),
        }

    aliq = Decimal(str(r.aliquota_iss or 0))
    if aliq > ALIQUOTA_ISS_MAXIMA:
        raise DadoIncompativel(
            f"Alíquota de ISS de {aliq}% não cabe no layout nacional, que aceita "
            f"no máximo {ALIQUOTA_ISS_MAXIMA}%. Confira a coluna Alíquota ISS da "
            f"obra na C. Diários."
        )

    discriminacao = (getattr(dados_rps, "discriminacao", "") or "").strip()
    if not discriminacao:
        raise DadoIncompativel("A discriminação do serviço está vazia — a nota não pode sair sem corpo.")
    if len(discriminacao) > 2000:
        raise DadoIncompativel(
            f"A discriminação tem {len(discriminacao)} caracteres e o layout aceita 2.000. "
            f"Encurte o texto na tela antes de emitir."
        )

    if not ibge_obra:
        raise DadoIncompativel(
            "O município da obra não foi resolvido para código IBGE — sem ele a "
            "prefeitura não sabe onde o serviço foi prestado."
        )

    agora = datetime.datetime.now(FUSO_BRASILIA).replace(microsecond=0)

    return nac.DadosDPS(
        serie=1,
        n_dps=numero_dps(numero_nota, ano),
        dh_emi=agora.isoformat(),
        # Competência é o MÊS; o dia 1 é convenção do layout. O mês continua
        # sendo o da emissão, como era no modelo antigo.
        d_compet=f"{ano:04d}-{mes:02d}-01",
        tp_amb=1 if producao else 2,
        # prestador: o CNPJ, a inscrição municipal e o código de Eusébio já vêm
        # como default no layout — são dados da própria BWS.
        prest_fone="8598322004",
        toma_doc=(dados_rps.toma_doc or ""),
        toma_nome=(dados_rps.toma_razao or ""),
        toma_cmun=int(dados_rps.toma_cmun or 0),
        toma_cep=(dados_rps.toma_cep or ""),
        toma_lgr=(dados_rps.toma_logradouro or ""),
        toma_nro=(dados_rps.toma_numero or ""),
        toma_bairro=(dados_rps.toma_bairro or ""),
        c_loc_prestacao=int(ibge_obra),
        c_trib_nac=(dados_rps.codigo_servico_nacional or "070202"),
        c_int_contrib=(dados_rps.codigo_tributacao_municipio or "702"),
        x_desc_serv=discriminacao,
        v_serv=_v(r.valor_total),
        v_ded_red=_v(deducao) if deducao > 0 else "",
        trib_issqn=1,                      # operação tributável
        tp_ret_issqn=tp_ret_iss,
        p_aliq=_v(aliq),
        v_ret_inss=_v(r.inss),
        v_ret_irrf=_v(r.ir if tem_ir else 0),
        v_ret_csll=_v(r.csll if tem_csll else 0),
        pis_cofins=pis_cofins,
    )
