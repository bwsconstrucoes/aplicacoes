# -*- coding: utf-8 -*-
"""
Fecha a parte NACIONAL de uma nota usando um XML nacional que você JÁ tem em mãos
(ex.: baixado do portal), SEM depender da varredura do ADN. Útil quando a nota
ainda não apareceu na distribuição do ADN mas você já possui o XML.

Para o número informado (que deve estar 'pendente' no Controle Nacional):
  [1] DANFSe nacional -> Drive
  [2] salva o XML nacional -> Drive
  [3] regenera a municipal COM a chave (mesmo arquivo/link)
  [4] completa o link nacional nos Links + marca concluído
  [5] Descrição do card com os 3 links

Depois disso o job_nacional não reprocessa a nota (status = concluído).

Uso:
  python fechar_nacional_manual.py <numero> <xml_nacional_path>
"""
from __future__ import annotations
import sys

from credenciais import cliente_gspread, ler_credenciais
import controle_nacional as ctrl
import drive_upload as drive
import notas_bws
import danfse
import nota_municipal
import pipefy_update as pf
from worker import ID_PROC, abrir_aba
from job_nacional import _dados_match, ABA_LINKS


def fechar(numero, xml_nac_path):
    gc = cliente_gspread()
    cred = ler_credenciais(gc)
    token = cred.get("PIPEFY_TOKEN", "")
    planilha = gc.open_by_key(ID_PROC)
    ws_links = abrir_aba(planilha, ABA_LINKS)

    with open(xml_nac_path, "rb") as fh:
        xml_nac = fh.read().decode("utf-8", "replace")
    dm = _dados_match(xml_nac)
    if not dm:
        print(">>> não consegui ler o XML nacional (estrutura inesperada). Abortado.")
        return
    print(f"XML nacional: chave {dm['chave']} | emit {dm['emit']} | toma {dm['toma']} | "
          f"compet {dm['dcompet']} | vServ {dm['vserv']}")

    pend = next((p for p in ctrl.listar_pendentes(planilha)
                 if str(p["numero"]).strip() == str(numero).strip()), None)
    if not pend:
        print(f">>> a nota {numero} não está como 'pendente' no Controle Nacional "
              f"(já concluída? número errado?). Abortado.")
        return

    # sanidade: confere se o XML é mesmo dessa nota
    if dm["emit"] != "00079526000109":
        print(f">>> ATENÇÃO: emitente do XML ({dm['emit']}) não é a BWS. Confirme o arquivo.")
    if _so_digitos(pend.get("toma_cnpj", "")) and dm["toma"] != _so_digitos(pend["toma_cnpj"]):
        print(f">>> ATENÇÃO: tomador do XML ({dm['toma']}) difere do pendente "
              f"({_so_digitos(pend['toma_cnpj'])}). Confirme se é a nota certa.")

    link_mun = pend.get("link_mun", "")
    nome_base = f"NOTA FISCAL {pend['numero']} - {pend['med']} ª Medição {pend['obra']}"
    print(f"\n===== FECHANDO NACIONAL (manual) — NOTA {numero} ({pend['obra']}) =====")

    # [1] DANFSe nacional
    try:
        danfse.gerar_danfse_pdf(xml_nac, "danfse_tmp.pdf")
        with open("danfse_tmp.pdf", "rb") as fh:
            _, link_nac = drive.enviar(f"{nome_base} (NFS-e Nacional).pdf", fh.read(), "pdf")
        print(f"[1] DANFSe nacional .... {link_nac}")
    except Exception as e:
        print(f"[1] DANFSe nacional .... ERRO: {e}")
        return

    # [2] XML nacional
    try:
        drive.enviar(f"{nome_base} (XML Nacional).xml", xml_nac.encode("utf-8"), "xml")
        print(f"[2] XML nacional ....... arquivado")
    except Exception as e:
        print(f"[2] XML nacional ....... ERRO: {e}")

    # [3] municipal com a chave (substitui o mesmo arquivo -> mesmo link)
    if pend.get("xml_abrasf_file_id"):
        try:
            xml_abrasf = drive.baixar(pend["xml_abrasf_file_id"]).decode("utf-8", "replace")
            nota_municipal.gerar_nota_municipal_pdf(xml_abrasf, "mun_tmp.pdf", xml_nacional=xml_nac)
            with open("mun_tmp.pdf", "rb") as fh:
                _, lk = drive.enviar(f"{nome_base} (NFS-e).pdf", fh.read(), "pdf")
            link_mun = lk or link_mun
            print(f"[3] Municipal c/ chave . regenerada (mesmo link)")
        except Exception as e:
            print(f"[3] Municipal c/ chave . ERRO: {e}")
    else:
        print(f"[3] Municipal c/ chave . pulada (sem xml_abrasf_file_id no pendente)")

    # [4] Links + marca concluído
    try:
        notas_bws.atualizar_links_nota(ws_links, pend["numero"], link_nacional=link_nac)
        ctrl.marcar_concluido(planilha, pend["numero"], link_nac, link_mun, dm["chave"])
        print(f"[4] Links + Controle ... nacional gravado, marcado concluído")
    except Exception as e:
        print(f"[4] Links + Controle ... ERRO: {e}")

    # [5] Descrição com os 3 links
    try:
        if token and pend.get("card_id"):
            pf.atualizar_descricao_links(pend["card_id"], link_mun, link_nac,
                                         pend.get("link_rec", ""), token)
            print(f"[5] Descrição .......... 3 links no topo")
    except Exception as e:
        print(f"[5] Descrição .......... ERRO: {e}")

    print("\n===== NACIONAL FECHADA — 4 documentos no Drive =====")


def _so_digitos(v) -> str:
    return "".join(c for c in str(v) if c.isdigit())


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("uso: python fechar_nacional_manual.py <numero> <xml_nacional_path>")
        sys.exit(1)
    try:
        fechar(sys.argv[1], sys.argv[2])
    except Exception as e:
        print(f"\n>>> ERRO GERAL: {type(e).__name__}: {e}")
