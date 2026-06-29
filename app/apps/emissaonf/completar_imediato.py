# -*- coding: utf-8 -*-
"""
Completa a ENTREGA da fase imediata de uma nota cujo BOOKKEEPING já foi feito
(Notas BWS, Omie e slots do Pipefy). Use quando o concluir.py rodou parcialmente
— ex.: falhou só no Drive. NÃO mexe em Notas BWS, Omie nem nos slots do Pipefy,
então é seguro rodar sem duplicar nada. É idempotente (pode repetir).

Faz:
  [a] arquiva o XML municipal no Drive
  [b] recibo -> Drive
  [c] municipal (sem chave, discriminação limpa) -> Drive
  [d] atualiza a linha de Notas BWS Links (municipal + recibo)
  [e] atualiza o pendente (link_mun, link_rec, file_id) p/ o job nacional
  [f] Descrição do card com os 2 links (NFS-e + Recibo)
  [g] WhatsApp (texto + recibo)

Uso:
  python completar_imediato.py <card_id> <numero> <codigo> <data_iso> <nota_xml_abrasf>
"""
from __future__ import annotations
import sys

from worker import preparar, abrir_aba, ID_PROC
import notas_bws
import drive_upload as drive
import controle_nacional as ctrl
import zapi
import efeitos
import recibo
import nota_municipal
import pipefy_update as pf
from preview import brl

ABA_LINKS = "Notas BWS Links"

_SENTINEL = object()


def completar(card_id, numero, codigo, data_iso, nota_xml_path,
              enviar_whatsapp: bool = True, discriminacao=_SENTINEL):
    ctx = preparar(card_id)
    card, obra, r = ctx["card"], ctx["obra"], ctx["r"]
    gc, cred = ctx["gc"], ctx["cred"]
    token = cred["PIPEFY_TOKEN"]
    # discriminação: por padrão usa a do ctx; na recuperação passe "" para usar a do XML
    discr_limpa = ((getattr(ctx.get("dados_rps"), "discriminacao", "") or "")
                   if discriminacao is _SENTINEL else (discriminacao or ""))
    obra_cod = card["codigo_obra"]
    med = card["numero_medicao"]
    nome_base = f"NOTA FISCAL {numero} - {med} ª Medição {obra_cod}"
    planilha = gc.open_by_key(ID_PROC)

    with open(nota_xml_path, "rb") as fh:
        xml_bytes = fh.read()
    xml_abrasf = xml_bytes.decode("utf-8", "replace")

    print(f"\n===== COMPLETAR ENTREGA — NOTA {numero} ({obra_cod}) =====")

    # [a] XML -> Drive
    xml_fid = ""
    try:
        xml_fid, _ = drive.enviar(f"{nome_base} (XML).xml", xml_bytes, "xml")
        print(f"[a] Drive XML .......... ok (id {xml_fid[:12]}...)")
    except Exception as e:
        print(f"[a] Drive XML .......... ERRO: {e}")

    # [b] recibo -> Drive
    link_rec = ""
    try:
        vbruto = nota_municipal.valor_bruto_nf(xml_abrasf)   # BRUTO da NF (do XML, não do card)
        dados_rec = efeitos.dados_recibo(card, obra, r, numero, valor_bruto=vbruto)
        recibo.gerar_recibo_pdf(dados_rec, "recibo_tmp.pdf")
        with open("recibo_tmp.pdf", "rb") as fh:
            _, link_rec = drive.enviar(f"{nome_base} (Recibo).pdf", fh.read(), "pdf")
        print(f"[b] Recibo ............. {link_rec}")
    except Exception as e:
        print(f"[b] Recibo ............. ERRO: {e}")

    # [c] municipal (sem chave) -> Drive
    link_mun = ""
    try:
        nota_municipal.gerar_nota_municipal_pdf(xml_abrasf, "mun_tmp.pdf",
                                                xml_nacional=None, discriminacao=discr_limpa or None)
        with open("mun_tmp.pdf", "rb") as fh:
            _, link_mun = drive.enviar(f"{nome_base} (NFS-e).pdf", fh.read(), "pdf")
        print(f"[c] Municipal .......... {link_mun}")
    except Exception as e:
        print(f"[c] Municipal .......... ERRO: {e}")

    # [d] Notas BWS Links (atualiza a linha existente)
    try:
        ws_links = abrir_aba(planilha, ABA_LINKS)
        ok = notas_bws.atualizar_links_nota(ws_links, numero,
                                            link_municipal=link_mun, link_recibo=link_rec)
        print(f"[d] Notas BWS Links .... {'atualizada' if ok else 'linha não encontrada'}")
    except Exception as e:
        print(f"[d] Notas BWS Links .... ERRO: {e}")

    # [e] pendente (link_mun, link_rec, file_id)
    try:
        ctrl.atualizar_pendente(planilha, numero, link_mun=link_mun,
                                link_rec=link_rec, xml_abrasf_file_id=xml_fid)
        print(f"[e] Pendente ........... atualizado")
    except Exception as e:
        print(f"[e] Pendente ........... ERRO: {e}")

    # [f] Descrição (links que existirem)
    try:
        if link_mun or link_rec:
            pf.atualizar_descricao_links(card["card_id"], link_mun, "", link_rec, token)
            n = sum(bool(x) for x in (link_mun, link_rec))
            print(f"[f] Descrição .......... {n} link(s) no topo")
    except Exception as e:
        print(f"[f] Descrição .......... ERRO: {e}")

    # [g] WhatsApp (texto + recibo)
    if not enviar_whatsapp:
        print("[g] WhatsApp ........... pulado (recuperação)")
    else:
      try:
        dest, aviso = efeitos._ler_destinatarios(gc)
        if aviso:
            print(f"[g] WhatsApp ........... {aviso}")
        msg = zapi.montar_mensagem(obra_cod, med, brl(r.valor_total),
                                   card.get("periodo_ini", ""), card.get("periodo_fim", ""), numero)
        envios = 0
        for dst in dest:
            if not zapi.deve_enviar(dst, obra_cod):
                continue
            if dst["tipo"] == "arquivo":
                if link_rec:
                    zapi.enviar_documento(cred, dst["telefone"], link_rec, nome_base + " (Recibo).pdf")
                    envios += 1
            else:
                zapi.enviar_texto(cred, dst["telefone"], msg)
                envios += 1
        print(f"[g] WhatsApp ........... {envios} envio(s)")
      except Exception as e:
        print(f"[g] WhatsApp ........... ERRO: {e}")

    print("\n===== ENTREGA COMPLETA — pronto p/ o job_nacional.py =====")


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("uso: python completar_imediato.py <card_id> <numero> <codigo> <data_iso> <nota_xml>")
        sys.exit(1)
    try:
        completar(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    except Exception as e:
        print(f"\n>>> ERRO GERAL: {type(e).__name__}: {e}")
