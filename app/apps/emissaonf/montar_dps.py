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


# Subitens da lista de serviços em que a plataforma nacional EXIGE o grupo de
# obra. A lista é a do próprio erro E0370, que a TI da prefeitura mostrou em
# 08/10/2026 — ela é a razão de a declaração 3281 ter sido aceita pelo município
# e recusada no nacional. A BWS emite sempre em 070202, então para ela o grupo é
# obrigatório em toda nota.
SUBITENS_QUE_EXIGEM_OBRA = frozenset({
    "070201", "070202", "070401", "070501", "070502", "070601", "070602",
    "070701", "070801", "071701", "071901", "141403", "141404",
})

# Quantos dígitos tem um CNO (e um CEI): doze. Serve só para avisar quando o
# número parece truncado — não barra, porque a recusa pela plataforma é
# informativa e barrar por palpite impediria uma obra legítima de faturar.
DIGITOS_CNO = 12


def _grupo_obra(obra, c_trib_nac: str):
    """Monta o grupo de obra a partir do CNO da C. Diários.

    **Por que o CNO, e não o endereço:** o layout aceita três identificações
    (CNO/CEI, CIB ou o endereço da obra) e exige exatamente uma. A C. Diários
    guarda o CNO de cada obra — é o dado que a BWS realmente tem, e ele já
    aparecia no texto da discriminação de todas as notas. Endereço da obra a
    planilha não tem (o que ela tem é o endereço do cliente, que é outra coisa),
    e CIB a empresa não usa.

    **O número vai sem pontuação**, como todo documento neste layout (o CNPJ, o
    CPF e o CEP também são enviados só com dígitos pelo próprio construtor).
    """
    if c_trib_nac not in SUBITENS_QUE_EXIGEM_OBRA:
        return None

    cno = "".join(filter(str.isdigit, str(getattr(obra, "cno", "") or "")))
    if not cno:
        raise DadoIncompativel(
            "A plataforma nacional EXIGE a identificação da obra para serviço de "
            f"construção civil (subitem {c_trib_nac[:2]}.{c_trib_nac[2:4]}.{c_trib_nac[4:]}), "
            "e esta obra está sem CNO na C. Diários. Preencha a coluna CNO da obra "
            "na planilha e emita de novo. É exatamente a falta disso que fez a "
            "declaração da nota 3281 ser aceita pela prefeitura e recusada no "
            "nacional (erro E0370)."
        )
    if len(cno) != DIGITOS_CNO:
        # Não barra: a plataforma é que valida o número contra a base da Receita,
        # e barrar por palpite impediria uma obra legítima de faturar. Mas sai no
        # log da emissão, porque CNO truncado é a explicação mais provável de uma
        # recusa com o grupo de obra presente.
        print(f"  >> ATENÇÃO: o CNO da obra tem {len(cno)} dígitos "
              f"(o normal são {DIGITOS_CNO}): {cno}")
    return nac.GrupoObra(c_obra=cno)


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

    # O grupo entra sempre que ALGUM dos três foi retido — inclusive quando só a
    # CSLL foi. Isso não é detalhe: o código `tpRetPisCofins` é o ÚNICO lugar da
    # declaração que diz quais dos três foram retidos. Sem ele, o valor da CSLL
    # viajaria sozinho, sem nada declarando que houve retenção.
    # Quando nenhum dos três é retido (categoria "SEM RETENÇÃO"), o grupo não vai
    # — mesmo comportamento do modelo antigo, que só mandava imposto retido.
    pis_cofins = None
    if tem_pis or tem_cofins or tem_csll:
        pis_cofins = {
            # CST descreve a situação da operação, não a retenção: serviço de
            # construção civil no Lucro Real é tributável à alíquota básica,
            # independente de quem recolhe.
            "CST": "01",
            "vBCPisCofins": _v(r.valor_total),
            "tpRetPisCofins": nac.tipo_retencao_pis_cofins(tem_pis, tem_cofins, tem_csll),
        }
        # Alíquota e valor só do que foi de fato retido. Mandar "0,00" num imposto
        # que não foi retido é diferente de não mandar: o primeiro declara uma
        # retenção de valor zero.
        if tem_pis:
            pis_cofins["pAliqPis"] = "0.65"
            pis_cofins["vPis"] = _v(r.pis)
        if tem_cofins:
            pis_cofins["pAliqCofins"] = "3.00"
            pis_cofins["vCofins"] = _v(r.cofins)

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

    c_trib_nac = (dados_rps.codigo_servico_nacional or "070202")

    # --- 4. O grupo de obra: o que faltava e derrubou a nota 3281 ---
    # O município aceita a declaração sem ele; a plataforma nacional recusa
    # (E0370). Como a recusa só aparece depois, do outro lado da fila, a nota
    # ficava "em processamento" para sempre. Barrar aqui, com o motivo escrito,
    # custa um aviso na tela; deixar passar custa um número de nota queimado.
    grupo_obra = _grupo_obra(obra, c_trib_nac)

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
        c_trib_nac=c_trib_nac,
        obra=grupo_obra,
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
