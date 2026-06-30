# -*- coding: utf-8 -*-
"""
Preview da nota: monta a discriminação no padrão BWS e gera um espelho visual
(HTML) parecido com a NFS-e da prefeitura, para conferência ANTES de emitir.
"""
from __future__ import annotations
from decimal import Decimal

MESES = ["", "JANEIRO", "FEVEREIRO", "MARÇO", "ABRIL", "MAIO", "JUNHO",
         "JULHO", "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO"]


def brl(v) -> str:
    s = f"{Decimal(str(v)):,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def _data_ext(ddmmaaaa: str) -> str:
    try:
        d, m, a = ddmmaaaa.split("/")
        return f"{int(d)} DE {MESES[int(m)]} DE {a}"
    except Exception:
        return ddmmaaaa


def _preenchido(v) -> bool:
    """False para vazio ou placeholders como '-', '--', 'N/A'."""
    s = str(v or "").strip()
    return bool(s) and s.strip("-").strip().upper() not in ("", "N/A", "NA")


def montar_discriminacao(card: dict, obra, r) -> str:
    base = r.base_servico
    # percentuais reais usados no cálculo (refletem sem-dedução e overrides)
    pct_inss_s = int((r.base_inss / base * 100).to_integral_value()) if base else 100
    pct_inss_m = 100 - pct_inss_s
    pct_iss_s = int((r.base_iss / base * 100).to_integral_value()) if base else 100
    pct_iss_m = 100 - pct_iss_s
    linhas = []
    linhas.append(
        f"PAGAMENTO DA {card['numero_medicao']}ª MEDIÇÃO DA {card['objeto']}, "
        f"CONFORME CONTRATO Nº {card['contrato']}. CNO Nº {obra.cno}."
    )
    if _preenchido(card.get("empenho")):
        linhas.append(f"EMPENHO Nº {card['empenho']}.")
    linhas.append(f"PERÍODO DA OBRA: {_data_ext(card['periodo_ini'])} À {_data_ext(card['periodo_fim'])}.")
    linhas.append(f"Valor da Nota: {brl(r.valor_total)}")
    if r.bdi_diferenciado > 0:
        linhas.append(f"Valor com BDI Diferenciado: {brl(r.bdi_diferenciado)}")
        linhas.append(f"Saldo sem BDI Diferenciado: {brl(base)}")
    # Base de cálculo: só mostra a do imposto que de fato é retido (INSS/ISS > 0).
    if r.inss > 0:
        linhas.append(
            f"Base de Cálculo INSS (Serviços {pct_inss_s}%: R$ {brl(r.base_inss)} - "
            f"Materiais {pct_inss_m}%: R$ {brl(base - r.base_inss)})"
        )
    if r.iss > 0:
        linhas.append(
            f"Base de Cálculo ISS (Serviços {pct_iss_s}%: R$ {brl(r.base_iss)} - "
            f"Materiais {pct_iss_m}%: R$ {brl(base - r.base_iss)})"
        )
    # tributos: lista os retidos + INSS + ISS (informativos não-retidos podem ser adicionados depois)
    if "PIS" in r.federais_retidos:
        linhas.append(f"PIS (0,65%): {brl(r.pis)}")
    if "COFINS" in r.federais_retidos:
        linhas.append(f"COFINS (3,00%): {brl(r.cofins)}")
    if "IR" in r.federais_retidos:
        linhas.append(f"IR (1,20%): {brl(r.ir)}")
    if "CSLL" in r.federais_retidos:
        linhas.append(f"CSLL (1,00%): {brl(r.csll)}")
    # valores dos impostos: só os que têm retenção aparecem (INSS/ISS = 0 são omitidos)
    if r.inss > 0:
        linhas.append(f"INSS (11,00%): {brl(r.inss)}")
    if r.iss > 0:
        linhas.append(f"ISS ({brl(r.aliquota_iss)}%): {brl(r.iss)}")
    banco = card.get("banco") or obra.conta_pagamento
    linhas.append(f"Conta p/ Pagamento: {banco}")
    if _preenchido(card.get("observacoes")):
        linhas.append(f"OBS.: {card['observacoes']}")
    return "\n".join(linhas)


def montar_preview_html(card: dict, obra, r, numero_rps, numero_nfse_esperado, ibge_obra,
                        tomador_end=None, discriminacao_override=None) -> str:
    disc_txt = discriminacao_override if discriminacao_override is not None else montar_discriminacao(card, obra, r)
    disc = disc_txt.replace("\n", "<br>")
    retidos = ", ".join(r.federais_retidos) or "—"
    if tomador_end:
        toma_razao = tomador_end.get("razao") or card['contratante']
        partes = [tomador_end.get("logradouro", ""), tomador_end.get("numero", ""),
                  tomador_end.get("bairro", ""),
                  f"{tomador_end.get('municipio','')}/{tomador_end.get('uf','')}",
                  f"CEP {tomador_end.get('cep','')}"]
        toma_endereco = " - ".join(p for p in partes if p and p not in ("/", "CEP "))
    else:
        toma_razao = card['contratante']
        toma_endereco = obra.endereco_cliente or "—"
    return f"""<!DOCTYPE html><html lang="pt-br"><head><meta charset="utf-8">
<style>
 body{{font-family:Arial,Helvetica,sans-serif;background:#eef1f4;margin:0;padding:24px;color:#1a2230}}
 .doc{{max-width:820px;margin:auto;background:#fff;border:1px solid #c4ccd6;box-shadow:0 2px 10px rgba(0,0,0,.08)}}
 .banner{{background:#b54708;color:#fff;text-align:center;font-weight:700;padding:8px;letter-spacing:1px}}
 .head{{text-align:center;padding:14px;border-bottom:2px solid #1a2230}}
 .head h1{{font-size:15px;margin:2px}}
 .head .sub{{font-size:12px;color:#566}}
 .sec{{border-bottom:1px solid #d7dde5;padding:10px 14px}}
 .sec h2{{font-size:11px;letter-spacing:.5px;color:#33415c;margin:0 0 6px;text-transform:uppercase}}
 .grid{{display:grid;grid-template-columns:1fr 1fr;gap:4px 18px;font-size:12.5px}}
 .grid div b{{color:#33415c}}
 .disc{{font-size:12px;line-height:1.55;white-space:normal}}
 table{{width:100%;border-collapse:collapse;font-size:12px;margin-top:4px}}
 td,th{{border:1px solid #d7dde5;padding:5px 7px;text-align:right}}
 th{{background:#f1f4f8;text-align:right;color:#33415c}}
 td.l,th.l{{text-align:left}}
 .liq{{font-size:16px;font-weight:700;color:#0a6c2f}}
 .tag{{display:inline-block;background:#eef2ff;color:#3949ab;border-radius:4px;padding:1px 6px;font-size:11px}}
</style></head><body><div class="doc">
 <div class="banner">PRÉ-VISUALIZAÇÃO — SIMULAÇÃO, NOTA NÃO EMITIDA</div>
 <div class="head">
   <h1>NOTA FISCAL DE SERVIÇOS ELETRÔNICA — NFS-e</h1>
   <div class="sub">Prefeitura Municipal de Eusébio · ABRASF 2.04 · RPS nº {numero_rps} · Nº NFS-e esperado: {numero_nfse_esperado}</div>
 </div>
 <div class="sec"><h2>Prestador</h2><div class="grid">
   <div><b>Razão Social:</b> BWS CONSTRUÇÕES LTDA</div><div><b>CNPJ:</b> 00.079.526/0001-09</div>
   <div><b>Inscrição Municipal:</b> 101084492</div><div><b>Regime:</b> Lucro Real · Não optante SN</div>
 </div></div>
 <div class="sec"><h2>Tomador</h2><div class="grid">
   <div><b>Razão Social:</b> {toma_razao}</div><div><b>CNPJ:</b> {card['cnpj_contratante']}</div>
   <div style="grid-column:1/3"><b>Endereço:</b> {toma_endereco}</div>
 </div></div>
 <div class="sec"><h2>Serviço</h2><div class="grid">
   <div><b>Item / Cód. Municipal:</b> 07.02 / 702</div><div><b>Exigibilidade ISS:</b> Exigível (1)</div>
   <div><b>Local da prestação:</b> {obra.municipio} (IBGE {ibge_obra})</div><div><b>Tributação:</b> <span class="tag">{obra.tributacao}</span></div>
 </div>
   <h2 style="margin-top:10px">Discriminação</h2><div class="disc">{disc}</div>
 </div>
 <div class="sec"><h2>Apuração</h2>
   <table>
     <tr><th class="l">Tributo</th><th>Base</th><th>Alíq.</th><th>Valor</th><th>Retido?</th></tr>
     <tr><td class="l">ISS</td><td>{brl(r.base_iss)}</td><td>{brl(r.aliquota_iss)}%</td><td>{brl(r.iss)}</td><td>{'Sim' if r.iss > 0 else 'Não'}</td></tr>
     <tr><td class="l">INSS</td><td>{brl(r.base_inss)}</td><td>11,00%</td><td>{brl(r.inss)}</td><td>{'Sim' if r.inss > 0 else 'Não'}</td></tr>
     <tr><td class="l">IR</td><td>{brl(r.valor_total)}</td><td>1,20%</td><td>{brl(r.ir)}</td><td>{'Sim' if 'IR' in r.federais_retidos else 'Não'}</td></tr>
     <tr><td class="l">PIS</td><td>{brl(r.valor_total)}</td><td>0,65%</td><td>{brl(r.pis)}</td><td>{'Sim' if 'PIS' in r.federais_retidos else 'Não'}</td></tr>
     <tr><td class="l">COFINS</td><td>{brl(r.valor_total)}</td><td>3,00%</td><td>{brl(r.cofins)}</td><td>{'Sim' if 'COFINS' in r.federais_retidos else 'Não'}</td></tr>
     <tr><td class="l">CSLL</td><td>{brl(r.valor_total)}</td><td>1,00%</td><td>{brl(r.csll)}</td><td>{'Sim' if 'CSLL' in r.federais_retidos else 'Não'}</td></tr>
   </table>
 </div>
 <div class="sec"><div class="grid">
   <div><b>Valor dos Serviços:</b> R$ {brl(r.valor_total)}</div><div><b>Total de Retenções:</b> R$ {brl(r.total_retencoes)}</div>
   <div><b>Federais retidos:</b> {retidos}</div><div class="liq">Valor Líquido: R$ {brl(r.valor_liquido)}</div>
 </div></div>
</div></body></html>"""
