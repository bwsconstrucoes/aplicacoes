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


def proximo_numero(gc) -> tuple[int, int]:
    ws = abrir_aba(gc.open_by_key(ID_PROC), ABA_NOTAS)
    col = ws.col_values(COL_NUMERO)
    nums = [int(re.sub(r"\D", "", c)) for c in col if re.sub(r"\D", "", c).isdigit()]
    ultimo = max(nums) if nums else 0
    return ultimo + 1, ultimo


def preparar(card_id: str, tipo_medicao_override=None, valor_override=None) -> dict:
    """Roda todo o pipeline até o XML assinado e devolve o contexto (sem efeitos).
    tipo_medicao_override / valor_override: usados na SUBSTITUIÇÃO, em que o card já
    não traz esses campos editáveis — injetam o valor antes do cálculo (reusa calcular)."""
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
    print(f"Obra: {card['codigo_obra']} | Medição {card['numero_medicao']} | "
          f"Valor {brl(card['valor_medicao'])} | BDI {brl(card['bdi'])}")

    obras = carregar_obras(abrir_aba(gc.open_by_key(ID_BASE), ABA_CDIARIOS).get_all_values())
    obra = buscar_obra(card["codigo_obra"], obras)
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
    prox, ultimo = proximo_numero(gc)

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
