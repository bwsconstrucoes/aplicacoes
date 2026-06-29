# -*- coding: utf-8 -*-
"""
Parte ADIADA do pós-emissão (Cron no Render). A nota nacional leva alguns minutos
pra subir pro ADN; este job pega as notas "aguardando nacional", e quando elas
aparecem na distribuição do ADN, gera a DANFSe nacional + o PDF municipal (com a
chave) e completa os links na planilha.

Fluxo:
  1) carrega certificado A1 + credenciais
  2) lê último NSU + notas pendentes (controle_nacional)
  3) varre o ADN a partir do NSU, filtra NFS-e em que a BWS é EMITENTE
  4) casa cada pendente por (tomador CNPJ + competência AAAA-MM + valor)
  5) gera DANFSe nacional + PDF municipal -> Drive -> links na planilha
  6) marca concluído e grava o novo NSU

Uso:
  python job_nacional.py            # produção
  python job_nacional.py RESTRITA   # ambiente de produção restrita (teste)
"""
from __future__ import annotations
import sys
import os
import xml.etree.ElementTree as ET

from credenciais import cliente_gspread, ler_credenciais
from el_nfse_abrasf import carregar_certificado_a1, carregar_certificado_auto
import adn_nfse
import controle_nacional as ctrl
import drive_upload as drive
import notas_bws
import danfse
import nota_municipal
import pipefy_update as pf
from worker import ID_PROC, abrir_aba

CNPJ_BWS = "00079526000109"
CERT_PATH = "certificado.p12"
ABA_LINKS = "Notas BWS Links"

# Composição do ID da DPS (padrão E&L/Eusébio), confirmada por engenharia reversa:
#   DPS + cLocEmi(7) + tpInsc(1) + CNPJ(14) + série(5) + nDPS(15)
# onde nDPS = AA (ano, 2) + número zero-pad(13). Ex.: nota 3074/2026 ->
#   DPS 2304285 2 00079526000109 00001 26 0000000003074
CLOC_EMI_EUSEBIO = "2304285"
SERIE_DPS = "00001"


def _id_dps(numero, ano2: str) -> str:
    """Monta o ID da DPS a partir do número da nota e do ano (2 dígitos)."""
    ndps = f"{ano2}{int(numero):013d}"   # AA + número(13) = 15 dígitos
    return f"DPS{CLOC_EMI_EUSEBIO}2{CNPJ_BWS}{SERIE_DPS}{ndps}"


def _ano2_do_pendente(pend) -> str:
    """Ano (2 dígitos) a partir da competência do pendente (YYYY-MM)."""
    import re as _re
    m = _re.search(r"(\d{4})", str(pend.get("dCompet") or pend.get("competencia") or ""))
    return m.group(1)[2:] if m else ""
BASE_RESTRITA = "https://adn.producaorestrita.nfse.gov.br/contribuintes"
NS = "{http://www.sped.fazenda.gov.br/nfse}"


def _so_digitos(v) -> str:
    return "".join(c for c in str(v) if c.isdigit())


def _dados_match(xml: str):
    """Extrai do XML nacional os campos usados pra casar com a emissão."""
    try:
        r = ET.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
    except Exception:
        return None
    inf = r.find(NS + "infNFSe")
    if inf is None:
        return None

    def g(path):
        e = inf.find(path)
        return (e.text or "").strip() if e is not None else ""

    return {
        "emit": _so_digitos(g(NS + "emit/" + NS + "CNPJ")),
        "toma": _so_digitos(g(".//" + NS + "toma/" + NS + "CNPJ")),
        "dcompet": g(".//" + NS + "dCompet")[:7],
        "vserv": float(g(".//" + NS + "vServ") or 0),
        "chave": (inf.get("Id") or "").replace("NFS", ""),
    }


def _casa(pend: dict, dm: dict) -> bool:
    if dm["emit"].zfill(14) != CNPJ_BWS.zfill(14):
        return False
    if dm["toma"].zfill(14) != _so_digitos(pend["toma_cnpj"]).zfill(14):
        return False
    if dm["dcompet"] != str(pend["dCompet"])[:7]:
        return False
    try:
        return abs(dm["vserv"] - float(pend["vServ"])) < 0.01
    except Exception:
        return False


def _fechar(planilha, ws_links, cred, pend, dm, xml_nac):
    """Fecha a parte nacional de uma pendente: DANFSe + XML nacional no Drive,
    regenera a municipal com a chave, completa links na planilha e a Descrição."""
    link_mun = pend.get("link_mun", "")
    nome_base = f"NOTA FISCAL {pend['numero']} - {pend['med']} ª Medição {pend['obra']}"

    danfse.gerar_danfse_pdf(xml_nac, "danfse_tmp.pdf")
    with open("danfse_tmp.pdf", "rb") as fh:
        _, link_nac = drive.enviar(f"{nome_base} (NFS-e Nacional).pdf", fh.read(), "pdf")
    drive.enviar(f"{nome_base} (XML Nacional).xml", xml_nac.encode("utf-8"), "xml")

    # regenera a MUNICIPAL agora COM a chave (mesmo nome -> mesmo link)
    if pend.get("xml_abrasf_file_id"):
        try:
            xml_abrasf = drive.baixar(pend["xml_abrasf_file_id"]).decode("utf-8", "replace")
            nota_municipal.gerar_nota_municipal_pdf(xml_abrasf, "mun_tmp.pdf", xml_nacional=xml_nac)
            with open("mun_tmp.pdf", "rb") as fh:
                _, lk = drive.enviar(f"{nome_base} (NFS-e).pdf", fh.read(), "pdf")
            link_mun = lk or link_mun
            print(f"  nota {pend['numero']}: municipal regenerada com a chave")
        except Exception as e:
            print(f"  nota {pend['numero']}: municipal NÃO regenerada — {type(e).__name__}: {e}")

    notas_bws.atualizar_links_nota(ws_links, pend["numero"], link_nacional=link_nac)
    ctrl.marcar_concluido(planilha, pend["numero"], link_nac, link_mun, dm["chave"])

    try:
        token = cred.get("PIPEFY_TOKEN", "")
        if token and pend.get("card_id"):
            pf.atualizar_descricao_links(pend["card_id"], link_mun, link_nac,
                                         pend.get("link_rec", ""), token)
            print(f"  nota {pend['numero']}: Descrição atualizada com o link nacional")
    except Exception as e:
        print(f"  nota {pend['numero']}: Descrição NÃO atualizada — {type(e).__name__}: {e}")

    print(f"  nota {pend['numero']}: CONCLUÍDA (nacional no Drive)")


def fechar_por_xml_nacional(xml_nac: str) -> bool:
    """Fecha a pendente correspondente a partir de um XML nacional fornecido
    (ex.: baixado do portal). NÃO usa ADN/NSU nem certificado — útil quando a
    nota já existe no nacional mas ainda não saiu na distribuição por NSU."""
    gc = cliente_gspread()
    cred = ler_credenciais(gc)
    planilha = gc.open_by_key(ID_PROC)

    dm = _dados_match(xml_nac)
    if not dm:
        print(">>> XML nacional inválido (não achei infNFSe / namespace nacional).")
        return False
    if dm["emit"] != CNPJ_BWS:
        print(f">>> XML nacional não é da BWS (emit={dm['emit']}).")
        return False
    print(f"XML nacional: toma={dm['toma']} dCompet={dm['dcompet']} "
          f"vServ={dm['vserv']:.2f} chave=...{dm['chave'][-8:]}")

    pendentes = ctrl.listar_pendentes(planilha)
    pend = next((p for p in pendentes if _casa(p, dm)), None)
    if not pend:
        print(">>> Nenhuma pendente casou com esse XML (talvez já concluída). "
              "Pendentes atuais: " + ", ".join(str(p.get("numero")) for p in pendentes))
        return False

    ws_links = abrir_aba(planilha, ABA_LINKS)
    try:
        _fechar(planilha, ws_links, cred, pend, dm, xml_nac)
        return True
    except Exception as e:
        print(f">>> ERRO ao fechar nota {pend['numero']}: {type(e).__name__}: {e}")
        return False


def fechar_por_chave(chave: str) -> bool:
    """Busca o XML nacional pela CHAVE na SEFIN (com o certificado) e fecha a
    pendente correspondente. Sem captcha, sem distribuição por NSU."""
    chave = "".join(c for c in (chave or "") if c.isdigit())
    if len(chave) != 50:
        print(f">>> Chave inválida: esperado 50 dígitos, veio {len(chave)}.")
        return False
    chave_pem, cert_pem = carregar_certificado_auto("", CERT_PATH)
    if not (chave_pem and cert_pem):
        print(">>> Certificado não carregado (env EMISSAO_NF_CERTIFICADO_P12_BASE64/SENHA).")
        return False
    try:
        xml_nac = adn_nfse.consultar_nfse_por_chave(cert_pem, chave_pem, chave)
    except Exception as e:
        print(f">>> Falha ao buscar nacional na SEFIN: {type(e).__name__}: {e}")
        return False
    print(f"Nacional obtido pela SEFIN ({len(xml_nac)} chars). Fechando pendente...")
    return fechar_por_xml_nacional(xml_nac)


def diag_dps_chave(chave: str) -> str:
    """Carrega o certificado e roda adn_nfse.diag_dps_por_chave — testa o caminho
    'só certificado' (buscar o nacional pelo ID da DPS, derivável do número)."""
    chave = "".join(c for c in (chave or "") if c.isdigit())
    if len(chave) != 50:
        return f">>> Chave inválida: esperado 50 dígitos, veio {len(chave)}."
    chave_pem, cert_pem = carregar_certificado_auto("", CERT_PATH)
    if not (chave_pem and cert_pem):
        return ">>> Certificado não carregado (env EMISSAO_NF_CERTIFICADO_P12_BASE64/SENHA)."
    return adn_nfse.diag_dps_por_chave(cert_pem, chave_pem, chave)


def diag_federal_chave(chave: str) -> str:
    """Carrega o certificado (igual o job) e roda adn_nfse.diag_por_chave —
    pra descobrir qual endpoint federal devolve a NFS-e/DANFSe nacional pela chave."""
    gc = cliente_gspread()
    cred = ler_credenciais(gc)
    senha = (cred.get("CERTIFICADO_SENHA") or cred.get("SENHA_CERTIFICADO") or cred.get("CERT_SENHA")
             or os.getenv("EMISSAO_NF_CERTIFICADO_SENHA") or os.getenv("CERTIFICADO_SENHA")
             or os.getenv("SENHA_CERTIFICADO") or os.getenv("CERT_SENHA"))
    if not senha:
        return ">>> Sem senha de certificado (env EMISSAO_NF_CERTIFICADO_SENHA)."
    chave_pem, cert_pem = carregar_certificado_auto(senha, CERT_PATH)
    if not (chave_pem and cert_pem):
        return ">>> Certificado não carregado (env EMISSAO_NF_CERTIFICADO_P12_BASE64)."
    chave = "".join(c for c in (chave or "") if c.isdigit())
    if len(chave) != 50:
        return f">>> Chave inválida: esperado 50 dígitos, veio {len(chave)}."
    return adn_nfse.diag_por_chave(cert_pem, chave_pem, chave)


def fechar_via_sefin(numeros=None) -> int:
    """Fecha pendentes buscando o nacional na SEFIN pelo ID da DPS (derivado do
    número) — só com o certificado, sem NSU, sem captcha, sem município.
    Para cada pendente: monta o ID da DPS -> /dps pega a chave -> /nfse pega o XML
    -> confere CNPJ+competência+valor (_casa) -> fecha. Devolve quantas fechou.
    `numeros`: lista opcional de números pra filtrar (usado na thread pós-emissão)."""
    chave_pem, cert_pem = carregar_certificado_auto("", CERT_PATH)
    if not (chave_pem and cert_pem):
        print(">>> [SEFIN] certificado não carregado — pulando via SEFIN.")
        return 0
    gc = cliente_gspread()
    cred = ler_credenciais(gc)
    planilha = gc.open_by_key(ID_PROC)
    pendentes = ctrl.listar_pendentes(planilha)
    if numeros:
        alvo = {str(n) for n in numeros}
        pendentes = [p for p in pendentes if str(p.get("numero")) in alvo]
    if not pendentes:
        return 0
    ws_links = abrir_aba(planilha, ABA_LINKS)
    print(f"[SEFIN] tentando {len(pendentes)} pendente(s) por DPS/chave...")
    fechadas = 0
    for pend in pendentes:
        num = pend.get("numero")
        ano2 = _ano2_do_pendente(pend)
        if not ano2:
            print(f"  nota {num}: sem competência pra montar o ID da DPS — pulei.")
            continue
        id_dps = _id_dps(num, ano2)
        try:
            chave = adn_nfse.consultar_chave_por_dps(cert_pem, chave_pem, id_dps)
        except Exception as e:
            print(f"  nota {num}: erro no /dps — {type(e).__name__}: {e}")
            continue
        if not chave:
            print(f"  nota {num}: ainda não consta na SEFIN (DPS {id_dps[-15:]}) — segue pendente.")
            continue
        try:
            xml_nac = adn_nfse.consultar_nfse_por_chave(cert_pem, chave_pem, chave)
        except Exception as e:
            print(f"  nota {num}: achei a chave mas falhou o /nfse — {type(e).__name__}: {e}")
            continue
        dm = _dados_match(xml_nac)
        if not dm:
            cab = xml_nac[:160].replace("\n", " ")
            print(f"  nota {num}: XML nacional não parseou (_dados_match=None). Início: {cab}")
            continue
        if not _casa(pend, dm):
            print(f"  nota {num}: não casou — "
                  f"XML(emit={dm['emit']} toma={dm['toma']} dcompet={dm['dcompet']} vserv={dm['vserv']}) "
                  f"vs pend(toma={_so_digitos(pend.get('toma_cnpj',''))} "
                  f"dcompet={str(pend.get('dCompet',''))[:7]} vserv={pend.get('vServ')})")
            continue
        try:
            _fechar(planilha, ws_links, cred, pend, dm, xml_nac)
            fechadas += 1
        except Exception as e:
            print(f"  nota {num}: ERRO ao fechar — {type(e).__name__}: {e}")
    print(f"[SEFIN] fechadas nesta passada: {fechadas}")
    return fechadas


def rodar(base: str = adn_nfse.BASE_PROD):
    gc = cliente_gspread()
    cred = ler_credenciais(gc)
    senha = (cred.get("CERTIFICADO_SENHA") or cred.get("SENHA_CERTIFICADO") or cred.get("CERT_SENHA")
             or os.getenv("EMISSAO_NF_CERTIFICADO_SENHA") or os.getenv("CERTIFICADO_SENHA")
             or os.getenv("SENHA_CERTIFICADO") or os.getenv("CERT_SENHA"))
    chave_pem, cert_pem = (None, None)
    if senha:
        chave_pem, cert_pem = carregar_certificado_auto(senha, CERT_PATH)
    if not (senha and chave_pem and cert_pem):
        print(">>> Sem certificado/senha (env CERTIFICADO_P12_BASE64 ou arquivo) — "
              "não dá pra consultar o ADN. Abortado.")
        return
    planilha = gc.open_by_key(ID_PROC)

    pendentes = ctrl.listar_pendentes(planilha)
    print(f"\n===== JOB NACIONAL =====")
    print(f"Notas aguardando nacional: {len(pendentes)}")
    if not pendentes:
        print("Nada a fazer.")
        return

    # 1) Caminho rápido: SEFIN por DPS/chave (sem fila do NSU). Fecha o que já
    #    estiver no nacional. Só recorre ao NSU pra quem sobrar.
    try:
        fechar_via_sefin()
    except Exception as e:
        print(f">>> [SEFIN] falhou geral (sigo pro NSU): {type(e).__name__}: {e}")
    pendentes = ctrl.listar_pendentes(planilha)
    if not pendentes:
        print("Tudo fechado pela SEFIN. NSU não foi necessário.")
        return
    print(f"Restaram {len(pendentes)} após a SEFIN — tentando pelo NSU (rede de segurança)...")

    nsu0 = ctrl.ler_nsu(planilha)
    print(f"Varrendo ADN a partir do NSU {nsu0} (base {base})...")
    res = adn_nfse.varrer_tudo(cert_pem, chave_pem, nsu0, CNPJ_BWS, base)
    docs = [d for d in res["docs"] if d.get("tipo") == "NFSE"]
    print(f"  {len(docs)} NFS-e na distribuição; NSU final {res['ultimo_nsu']}")

    # só as NFS-e em que a BWS é EMITENTE
    nacionais = []
    for d in docs:
        dm = _dados_match(d.get("xml", ""))
        if dm and dm["emit"] == CNPJ_BWS:
            nacionais.append((dm, d))
    print(f"  {len(nacionais)} emitidas pela BWS")
    for dm, _ in nacionais:
        try:
            print(f"    ADN BWS: toma={dm['toma']} dCompet={dm['dcompet']} "
                  f"vServ={dm['vserv']:.2f} chave=...{dm['chave'][-8:]}")
        except Exception:
            pass

    ws_links = abrir_aba(planilha, ABA_LINKS)
    concluidas = 0
    for pend in pendentes:
        match = next((nd for nd in nacionais if _casa(pend, nd[0])), None)
        if not match:
            try:
                pv = f"{float(pend['vServ']):.2f}"
            except Exception:
                pv = pend.get("vServ")
            print(f"  nota {pend['numero']}: ainda não casou — "
                  f"pend(toma={_so_digitos(pend.get('toma_cnpj',''))} "
                  f"dCompet={str(pend.get('dCompet',''))[:7]} vServ={pv})")
            continue
        dm, doc = match
        try:
            _fechar(planilha, ws_links, cred, pend, dm, doc["xml"])
            concluidas += 1
        except Exception as e:
            print(f"  nota {pend['numero']}: ERRO — {type(e).__name__}: {e}")

    ctrl.gravar_nsu(planilha, res["ultimo_nsu"])
    print(f"\nNSU atualizado para {res['ultimo_nsu']}. Concluídas nesta rodada: {concluidas}")


if __name__ == "__main__":
    base = BASE_RESTRITA if (len(sys.argv) > 1 and sys.argv[1].strip().upper() == "RESTRITA") \
        else adn_nfse.BASE_PROD
    try:
        rodar(base)
    except Exception as e:
        print(f"\n>>> ERRO GERAL: {type(e).__name__}: {e}")
