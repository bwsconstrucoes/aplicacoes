# -*- coding: utf-8 -*-
"""
Worker de simulação da emissão de NFS-e da BWS, a partir de um ID de card do Pipefy.

Modo SIMULAÇÃO (padrão): lê tudo (Pipefy, C. Diários, numeração), calcula, gera o
preview visual, e LISTA o que faria — mas NÃO emite e NÃO grava em lugar nenhum.

Uso:
    python worker.py <ID_DO_CARD>

Requisitos no ambiente: credenciais.json (service account com acesso às 3 planilhas),
e PIPEFY_TOKEN na aba 'Credenciais'.  pip install gspread requests signxml cryptography lxml
"""
from __future__ import annotations
import os
import re
import sys
import datetime

from credenciais import cliente_gspread, ler_credenciais
from pipefy import get_card, extrair_card
from cdiarios import carregar_obras, buscar_obra
from tributacao import (parse_categoria, calcular, valor_base_nota, overrides_do_card,
                        CategoriaInvalida, DadoObrigatorioAusente)
from municipios_ibge import carregar_cache, resolver
from preview import montar_preview_html, brl
from montar_emissao import montar_dados_rps, gerar_xml_preview
from el_nfse_abrasf import carregar_certificado_a1, carregar_certificado_auto
import efeitos

# --- planilhas ---
ID_BASE = "1C7MWQmr5uFGWuJ18osUNDapiojVXzQ_GxMMDQqxPsBk"   # C. Diários
ABA_CDIARIOS = ["Centro de Custo", "Centro de Custos", "C. Diários", "C. Diarios", "C.Diários"]
ID_PROC = "1NOEzey3vKleRuX7Jm8GylRBjGDFmQYi5l0LxtpPpEbU"   # Notas BWS (numeração)
ABA_NOTAS = "Notas BWS"
COL_NUMERO = 6   # coluna F = nº da última nota
CERT_PATH = "certificado.p12"   # certificado A1 na pasta do script


def _norm_cod(v) -> str:
    """Normaliza um código de obra para comparação (sem espaços, maiúsculo)."""
    return str(v or "").strip().upper()


def abrir_aba(planilha, candidatos):
    """Abre a 1ª aba que casar (ignorando espaços/maiúsculas) ou mostra as disponíveis."""
    if isinstance(candidatos, str):
        candidatos = [candidatos]
    abas = {ws.title.strip().lower(): ws for ws in planilha.worksheets()}
    for nome in candidatos:
        ws = abas.get(nome.strip().lower())
        if ws:
            return ws
    raise KeyError(
        f"Nenhuma aba {candidatos} encontrada. Abas disponíveis: {[ws.title for ws in planilha.worksheets()]}"
    )


def proximo_numero(gc, card_id=None) -> tuple[int, int]:
    """Próximo número da nota, e o último de fato emitido.

    O próximo sai do maior número da planilha MAIS **todos** os números que já
    tiveram declaração enviada, qualquer que seja o desfecho dela. As duas
    parcelas vieram de defeitos reais, e a segunda mudou de forma em 08/10/2026:

    **07/10/2026** — uma declaração que a prefeitura aceitou e que ainda não
    virou nota **não entra na planilha**, porque a planilha só recebe nota
    pronta. O número dela ficava "livre" para a próxima emissão enquanto a
    prefeitura o mantinha reservado: a nota seguinte sairia pedindo o mesmo
    número, e a prefeitura leria isso como reenvio da anterior — dois serviços
    colapsados num documento.

    **08/10/2026** — a versão de 07/10 ainda reaproveitava o número para o MESMO
    card, porque o manual diz que declaração recusada pode ser reenviada com a
    mesma identificação. Em Eusébio não é assim: a declaração da 3281 foi aceita,
    transmitida, recusada no nacional, teve o número liberado e reusado — e a
    prefeitura respondeu **EL99, "chave informada para a DPS não existe no
    repositório municipal"**. Número já enviado é número gasto, e a exceção do
    mesmo card saiu.

    Pular um número deixa buraco na sequência, e isso é normal — nota cancelada
    faz o mesmo. Reusar um número gasta uma emissão e horas até descobrir.
    """
    planilha = gc.open_by_key(ID_PROC)
    ws = abrir_aba(planilha, ABA_NOTAS)
    col = ws.col_values(COL_NUMERO)
    nums = [int(re.sub(r"\D", "", c)) for c in col if re.sub(r"\D", "", c).isdigit()]
    ultimo = max(nums) if nums else 0        # o último REALMENTE emitido

    presos = []
    try:
        import declaracoes
        presos = declaracoes.numeros_registrados(planilha)
    except Exception as e:
        print(f"  [aviso] não consegui ler as declarações já enviadas "
              f"({type(e).__name__}: {e}) — o número pode colidir com uma delas.")

    prox = max([ultimo] + presos) + 1
    if presos and prox > ultimo + 1:
        print(f"  >> número {ultimo + 1} já foi usado numa declaração enviada "
              f"(maior enviado: {max(presos)}); esta nota vai sair como {prox}.")
    return prox, ultimo


def preparar(card_id: str, tipo_medicao_override=None, valor_override=None,
             nota_substituida=None) -> dict:
    """Roda todo o pipeline até o XML assinado e devolve o contexto (sem efeitos).
    tipo_medicao_override / valor_override: usados na SUBSTITUIÇÃO, em que o card já
    não traz esses campos editáveis — injetam o valor antes do cálculo (reusa calcular).
    nota_substituida: se informado e sem valor_override, o valor-base default passa a ser
    o da nota antiga (a que está sendo substituída)."""
    print(f"=== Card {card_id} ===\n")
    gc = cliente_gspread()
    cred = ler_credenciais(gc)
    token = cred.get("PIPEFY_TOKEN")
    if not token:
        raise KeyError("PIPEFY_TOKEN não encontrado na aba 'Credenciais'.")
    senha_cert = (cred.get("CERTIFICADO_SENHA") or cred.get("SENHA_CERTIFICADO") or cred.get("CERT_SENHA")
                  or os.getenv("EMISSAO_NF_CERTIFICADO_SENHA") or os.getenv("CERTIFICADO_SENHA")
                  or os.getenv("SENHA_CERTIFICADO") or os.getenv("CERT_SENHA"))

    card = extrair_card(get_card(card_id, token))
    # override de tipo de medição (substituição): entra antes do cálculo de retenções
    if tipo_medicao_override:
        card["tipo_medicao"] = tipo_medicao_override
    # substituição sem valor explícito: o valor-base default é o da nota antiga
    if valor_override is None and nota_substituida:
        import validacao as _v
        _alvo = "".join(c for c in str(nota_substituida) if c.isdigit())
        for _s in _v.slots_preenchidos(card):
            if "".join(c for c in str(_s["numero"]) if c.isdigit()) == _alvo:
                valor_override = f"{_s['valor']:.2f}"
                break
    print(f"Obra: {card['codigo_obra']} | Medição {card['numero_medicao']} | "
          f"Valor {brl(card['valor_medicao'])} | BDI {brl(card['bdi'])}")

    obras = carregar_obras(abrir_aba(gc.open_by_key(ID_BASE), ABA_CDIARIOS).get_all_values())
    obra = buscar_obra(card["codigo_obra"], obras)
    # A obra tem dois códigos na planilha; dizer por qual dos dois ela foi achada
    # evita caçada quando o card e a C. Diários usam códigos diferentes.
    if obra.codigo_primario and _norm_cod(card["codigo_obra"]) != _norm_cod(obra.codigo_primario):
        print(f"  >> obra achada pelo código SECUNDÁRIO '{card['codigo_obra']}' "
              f"(o primário dela é '{obra.codigo_primario}')")
    print(f"Tributação: {obra.tributacao} | Alíq. ISS: {obra.aliquota_iss} | Município: {obra.municipio}")

    cat = parse_categoria(obra.tributacao)
    base_valor = valor_override or valor_base_nota(card)
    ov = overrides_do_card(card)
    r = calcular(base_valor, cat, aliquota_iss=obra.aliquota_iss,
                 bdi_diferenciado=card["bdi"], iss_retido=True, overrides=ov)
    if str(base_valor) != str(card["valor_medicao"]):
        print(f"  >> VALOR PARCIAL: nota sobre {brl(base_valor)} (medição é {brl(card['valor_medicao'])})")
    if ov.sem_deducao:
        print("  >> Tipo de Medição: REAJUSTE SEM DEDUÇÃO → 100% serviço (sem dedução de materiais)")
    if ov.usar_aliquotas or ov.usar_deducoes:
        print(f"  >> OVERRIDES ativos (ignora C. Diários): alíquotas={ov.usar_aliquotas} | deduções/ISS={ov.usar_deducoes}")
    ibge = resolver(obra.municipio, carregar_cache())
    prox, ultimo = proximo_numero(gc, card_id=card_id)

    data_emissao = datetime.date.today().isoformat()
    dados_rps, avisos, end_tom = montar_dados_rps(card, obra, r, prox, ibge, data_emissao, carregar_cache())

    html = montar_preview_html(card, obra, r, numero_rps=prox,
                               numero_nfse_esperado=prox, ibge_obra=ibge, tomador_end=end_tom)
    with open("preview_nota.html", "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"\nPreview gerado: preview_nota.html")
    print(f"INSS {brl(r.inss)} | ISS {brl(r.iss)} | retenções {brl(r.total_retencoes)} | "
          f"LÍQUIDO R$ {brl(r.valor_liquido)}")
    print(f"Numeração: última emitida {ultimo} → próxima esperada {prox}")

    print(f"\nTomador: doc={dados_rps.toma_doc!r} | razão={dados_rps.toma_razao!r}")
    print(f"  endereço: {dados_rps.toma_logradouro!r}, {dados_rps.toma_numero!r} - "
          f"{dados_rps.toma_bairro!r} - cMun {dados_rps.toma_cmun!r}/{dados_rps.toma_uf!r} - CEP {dados_rps.toma_cep!r}")
    for a in avisos:
        print(f"  [aviso] {a}")
    chave_pem = cert_pem = None
    if senha_cert:
        try:
            chave_pem, cert_pem = carregar_certificado_auto(senha_cert, CERT_PATH)
            if not (chave_pem and cert_pem):
                print("  [aviso] certificado A1 não encontrado (env CERTIFICADO_P12_BASE64 "
                      "nem arquivo); XML sem assinatura")
        except Exception as e:
            print(f"  [aviso] não assinei o XML ({e}); estrutura sem assinatura")
    else:
        print("  [aviso] CERTIFICADO_SENHA não está na aba Credenciais; XML sem assinatura")
    xml = gerar_xml_preview(dados_rps, chave_pem, cert_pem)

    return {"card": card, "obra": obra, "r": r, "ibge": ibge, "prox": prox, "ultimo": ultimo,
            "dados_rps": dados_rps, "avisos": avisos, "end_tom": end_tom, "xml": xml,
            "assinado": bool(chave_pem and cert_pem), "chave_pem": chave_pem, "cert_pem": cert_pem,
            "senha_cert": senha_cert, "gc": gc, "cred": cred}


def simular(card_id: str, gravar: bool = False):
    ctx = preparar(card_id)
    print("\n===== XML QUE SERIA ENVIADO (ABRASF GerarNfse) — DRY-RUN, NADA FOI EMITIDO =====")
    print(ctx["xml"])
    print("\n[EMISSÃO] DRY-RUN — o XML acima NÃO foi enviado ao webservice do Eusébio.")
    print(f"  Na produção: envia, recebe o nº da NFS-e e valida se == {ctx['prox']} (alerta se divergir).")
    efeitos.simular(ctx["card"], ctx["obra"], ctx["r"], ctx["prox"], ctx["gc"])
    if gravar:
        print("\n(gravar=True ainda não implementado — produção entra peça por peça)")
    return ctx["r"]


if __name__ == "__main__":
    card_id = sys.argv[1] if len(sys.argv) > 1 else "1384982344"
    try:
        simular(card_id, gravar=False)
    except CategoriaInvalida as e:
        print(f"\n>>> CRÍTICA (categoria fora do padrão) — emissão BARRADA:\n    {e}")
        print("    Corrija a coluna Tributação na C. Diários e rode de novo.")
    except DadoObrigatorioAusente as e:
        print(f"\n>>> CRÍTICA (dado obrigatório) — emissão BARRADA:\n    {e}")
    except KeyError as e:
        print(f"\n>>> ERRO: {e}")
