# -*- coding: utf-8 -*-
"""
Costura de todos os efeitos colaterais da emissão, em MODO SIMULAÇÃO:
monta cada payload (recibo, Dropbox, Pipefy, Omie, WhatsApp, Notas BWS Links)
e mostra na tela o que faria — sem executar rede nem gravar nada.

Quando formos para produção, cada bloco vira a chamada real correspondente.
"""
from __future__ import annotations

import recibo
import dropbox_client as dbx
import pipefy_update as pf
import omie
import zapi
import notas_bws
from preview import brl
from credenciais import SHEET_CREDENCIAIS

MESES = ["", "janeiro", "fevereiro", "março", "abril", "maio", "junho",
         "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
CIDADE_BWS = "Fortaleza-CE"


def _data_ext(iso):  # "2026-06-27" -> "27 de junho de 2026"
    try:
        a, m, d = iso.split("-")
        return f"{int(d)} de {MESES[int(m)]} de {a}"
    except Exception:
        return iso


def dados_recibo(card, obra, r, numero, valor_bruto=None):
    data_ext = _data_ext(__import__("datetime").date.today().isoformat())
    p = data_ext.split(" de ")
    if len(p) == 3:                      # capitaliza o mês: "junho" -> "Junho"
        p[1] = p[1].capitalize()
        data_ext = " de ".join(p)
    tomador = card.get("contratante", "") or getattr(obra, "cliente", "") or ""
    return {
        "cidade": "Eusébio",
        "data_extenso": data_ext,
        "tomador": tomador,
        "contrato": card.get("contrato", ""),
        "objeto": card.get("objeto", ""),
        "valor": valor_bruto if valor_bruto is not None else r.valor_total,   # BRUTO da NF emitida
        "medicao": card.get("numero_medicao", ""),
        "empenho": card.get("empenho", ""),
        "id_documento": card.get("card_id", ""),
    }


def _ler_destinatarios(gc):
    plan = gc.open_by_key(SHEET_CREDENCIAIS)
    # tenta o nome configurado e variações comuns (acento, sem sufixo)
    candidatos = [zapi.ABA_DESTINATARIOS, "Destinatarios WhatsApp", "Destinatários WhatsApp",
                  "Destinatarios", "Destinatários", "Destinatarios Whatsapp"]
    vistos, abas = set(), []
    for nome in candidatos:
        if nome and nome not in vistos:
            vistos.add(nome); abas.append(nome)
    for nome in abas:
        try:
            ws = plan.worksheet(nome)
            return zapi.carregar_destinatarios(ws.get_all_values()), None
        except Exception:
            continue
    nomes_existentes = []
    try:
        nomes_existentes = [w.title for w in plan.worksheets()]
    except Exception:
        pass
    return [], (f"não achei a aba de destinatários (tentei {abas}). "
                f"Abas existentes: {nomes_existentes}")


def linha_notas_bws_links(numero, obra, ano, nome_base):
    # headers: Nº NOTA FISCAL - OBRA | Nº NOTA FISCAL | ANO | OBRA | NOME DO ARQUIVO | LINK NOTA | LINK RECIBO
    return [f"{numero} - {obra}", numero, ano, obra, nome_base, "<link nota>", "<link recibo>"]


def simular(card, obra, r, numero, gc):
    obra_cod = card.get("codigo_obra", "")
    med = card.get("numero_medicao", "")
    ano = __import__("datetime").date.today().year
    nome_base = f"NOTA FISCAL {numero} - {med} ª Medição {obra_cod}"

    print("\n========== EFEITOS COLATERAIS (SIMULAÇÃO — nada executado) ==========")

    # 1) RECIBO
    d = dados_recibo(card, obra, r, numero)
    html = recibo.montar_recibo_html(d)
    with open("recibo_simulado.html", "w", encoding="utf-8") as fh:
        fh.write(html)
    print("\n[1] RECIBO — gerado 'recibo_simulado.html' (na produção: converte p/ PDF via Playwright)")

    # 2) DROPBOX
    nf_path = f"{dbx.PASTA}/{dbx.nome_arquivo(numero, med, obra_cod, 'NF')}.pdf"
    rec_path = f"{dbx.PASTA}/{dbx.nome_arquivo(numero, med, obra_cod, 'Recibo')}.pdf"
    print("\n[2] DROPBOX — subiria 2 arquivos e geraria links:")
    print(f"    NF     : {nf_path}")
    print(f"    Recibo : {rec_path}")

    # 3) PIPEFY
    try:
        slot = pf.detectar_slot(card.get("campos_raw", {}))
        mut = pf.montar_mutation(card["card_id"], slot,
                                 numero, __import__("datetime").date.today().strftime("%d/%m/%Y"),
                                 r.valor_total, r.valor_liquido)
        print(f"\n[3] PIPEFY — slot da nota: {slot}  (preencheria status/data/nº/valor/líquido + limpa campos + etiqueta)")
        print(f"    mutation com {mut.count('updateCardField')} campos")
    except pf.TodasNotasPreenchidas as e:
        print(f"\n[3] PIPEFY — ATENÇÃO: {e}")

    # 4) OMIE
    par = omie.montar_param_retencoes(card.get("omie_integracao", ""), r, numero)
    print(f"\n[4] OMIE — AlterarContaReceber (título {card.get('omie_integracao','')}):")
    print(f"    INSS {par['valor_inss']} | ISS {par['valor_iss']}({par['retem_iss']}) | "
          f"IR {par['valor_ir']}({par['retem_ir']}) | PIS {par['valor_pis']}({par['retem_pis']}) | "
          f"COFINS {par['valor_cofins']}({par['retem_cofins']}) | CSLL {par['valor_csll']}({par['retem_csll']})")

    # 5) WHATSAPP
    dest, aviso = _ler_destinatarios(gc)
    print("\n[5] WHATSAPP (Z-API):")
    if aviso:
        print(f"    [aviso] {aviso}")
    msg = zapi.montar_mensagem(obra_cod, med, brl(r.valor_total),
                               card.get("periodo_ini", ""), card.get("periodo_fim", ""), numero)
    enviam = [d2 for d2 in dest if zapi.deve_enviar(d2, obra_cod)]
    for d2 in enviam:
        tipo = "ARQUIVO (recibo)" if d2["tipo"] == "arquivo" else "texto"
        print(f"    -> {d2['nome']} ({d2['telefone']}) [{tipo}]")
    if not dest:
        print("    (nenhum destinatário carregado — crie a aba 'Destinatarios WhatsApp')")
    print("    --- mensagem ---")
    for ln in msg.splitlines():
        print(f"    {ln}")

    # 6) NOTAS BWS (linha principal A–P; Q–BA são fórmulas, não escritas)
    iso = __import__("datetime").date.today().isoformat()
    linha = notas_bws.montar_linha(card, obra, r, numero, iso)
    cols = "A B C D E F G H I J K L M N O P".split()
    print("\n[6] NOTAS BWS — adicionaria a linha (colunas A–P; Q–BA preenchem por fórmula):")
    print("    " + " | ".join(f"{c}={v}" for c, v in zip(cols, linha) if v != ""))

    # 7) NOTAS BWS LINKS
    linha_links = linha_notas_bws_links(numero, obra_cod, ano, nome_base)
    print("\n[7] NOTAS BWS LINKS — adicionaria a linha:")
    print(f"    {linha_links}")
    print("\n=====================================================================")
