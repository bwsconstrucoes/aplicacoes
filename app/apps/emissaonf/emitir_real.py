# -*- coding: utf-8 -*-
"""
Emissão REAL da NFS-e (ABRASF GerarNfse) para um card do Pipefy.

  python emitir_real.py <ID_CARD>            -> PREVIEW (monta a nota assinada e mostra
                                                 o envelope SOAP; NÃO envia nada)
  python emitir_real.py <ID_CARD> ENVIAR     -> ENVIA de verdade ao Eusébio

Mesmo no envio real, SÓ a nota é emitida. Os efeitos colaterais (Pipefy, Omie,
Dropbox, WhatsApp, planilhas) continuam desligados — ligamos um a um depois.
"""
from __future__ import annotations
import os
import sys
import tempfile

from worker import preparar
import el_nfse_envio as envio


def _cert_temp(cert_pem: bytes, chave_pem: bytes):
    cf = tempfile.NamedTemporaryFile("wb", suffix=".pem", delete=False); cf.write(cert_pem); cf.close()
    kf = tempfile.NamedTemporaryFile("wb", suffix=".pem", delete=False); kf.write(chave_pem); kf.close()
    return cf.name, kf.name


def emitir(card_id: str, mandar: bool = False):
    ctx = preparar(card_id)
    xml, prox = ctx["xml"], ctx["prox"]

    if not ctx["assinado"]:
        print("\n>>> O XML NÃO foi assinado (certificado ou senha ausente). Emissão abortada.")
        print("    Confira o certificado.p12 na pasta e a chave CERTIFICADO_SENHA na aba Credenciais.")
        return

    if not mandar:
        print()
        envio.enviar(xml, de_verdade=False, incluir_cabec=True)
        print("\n>>> Isso foi só PREVIEW — NADA foi enviado.")
        print(">>> Confira o envelope acima. Para EMITIR DE VERDADE, rode no terminal:")
        print(f"        python emitir_real.py {card_id} ENVIAR")
        return

    cert_path, key_path = _cert_temp(ctx["cert_pem"], ctx["chave_pem"])
    try:
        print(f"\n>>> ENVIANDO ao webservice do Eusébio (RPS/nº esperado: {prox})...")
        resp = envio.enviar(xml, de_verdade=True, incluir_cabec=True, cert=(cert_path, key_path))
    finally:
        for p in (cert_path, key_path):
            try:
                os.unlink(p)
            except OSError:
                pass

    print(f"HTTP {resp.status_code}")
    res = envio.parse_resposta(resp.text)
    if res["numero"]:
        print(f"\n✅ NFS-e EMITIDA — número {res['numero']} | código {res['codigo_verificacao']} | emissão {res['data_emissao']}")
        if str(res["numero"]) != str(prox):
            print(f"   ⚠️  número devolvido ({res['numero']}) != esperado ({prox}) — confira a numeração na Notas BWS.")
        else:
            print(f"   número confere com o esperado ({prox}).")
        # salva a nota oficial (XML) e o resultado, para os próximos passos
        try:
            if res.get("nota_xml"):
                with open(f"NFSe_{res['numero']}.xml", "w", encoding="utf-8") as fh:
                    fh.write(res["nota_xml"])
            with open(f"emissao_{res['numero']}.json", "w", encoding="utf-8") as fh:
                import json
                json.dump({"card_id": card_id, "numero": res["numero"],
                           "codigo_verificacao": res["codigo_verificacao"],
                           "data_emissao": res["data_emissao"]}, fh, ensure_ascii=False, indent=1)
            print(f"   nota oficial salva em NFSe_{res['numero']}.xml")
        except Exception as ex:
            print(f"   [aviso] não consegui salvar a nota em arquivo: {ex}")
        print("\n>>> Efeitos colaterais seguem DESLIGADOS. Me cole esta saída que ligamos um a um.")
    else:
        print("\n❌ A prefeitura NÃO retornou número de NFS-e. Mensagens:")
        for e in res["erros"]:
            print("   -", e)
        print("\n>>> Nenhuma nota válida foi gerada.")
    print("\n----- resposta bruta (guarde / me cole) -----")
    print(resp.text[:3500])


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("uso: python emitir_real.py <ID_CARD> [ENVIAR]")
        sys.exit(1)
    cid = sys.argv[1]
    mandar = len(sys.argv) > 2 and sys.argv[2].strip().upper() == "ENVIAR"
    try:
        emitir(cid, mandar=mandar)
    except Exception as e:
        print(f"\n>>> ERRO: {type(e).__name__}: {e}")