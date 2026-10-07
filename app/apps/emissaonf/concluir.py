# -*- coding: utf-8 -*-
"""
Parte IMEDIATA do pós-emissão (roda logo após emitir). Faz tudo que NÃO depende
da nacional — incluindo já gerar a MUNICIPAL (sem a chave, que ainda não existe;
a nota vale pelo código de verificação) e o RECIBO:

  [1] Notas BWS (linha A–P)
  [2] Omie (retenções, acúmulo)
  [3] Pipefy: slots A–E + LIMPEZA dos 12 campos de entrada
  [4] Drive: arquiva o XML municipal
  [5] Recibo -> Drive (+link)
  [6] Municipal NF-e (sem chave) -> Drive (+link)
  [7] Notas BWS Links (municipal + recibo)
  [8] Pipefy: Descrição com os 2 links (NFS-e + Recibo) no topo
  [9] WhatsApp (texto + recibo)
  [10] Registra "aguardando nacional" (o job gera a DANFSe nacional, regenera a
       municipal já com a chave e completa a Descrição com o link nacional)

Uso:
  python concluir.py <card_id> <numero> <codigo> <data_iso> <nota_xml_abrasf> [FORCAR]
"""
from __future__ import annotations
import sys

from worker import preparar, abrir_aba, ID_PROC, ABA_NOTAS
import notas_bws
import omie
import pipefy_update as pf
import drive_upload as drive
import controle_nacional as ctrl
import zapi
import efeitos
import recibo
import nota_municipal
import validacao
from tributacao import calcular, parse_categoria, overrides_do_card
from preview import brl

ABA_LINKS = "Notas BWS Links"


def concluir(card_id, numero, codigo, data_iso, nota_xml_path, forcar=False, ctx=None,
             nota_substituida=None, nacional=False, chave_nacional=""):
    """Pós-emissão imediato.

    `nacional=True` quer dizer que o XML em `nota_xml_path` é o da NFS-e
    NACIONAL — o que a prefeitura passou a devolver em 07/10/2026. Nesse caso a
    nota já nasce completa: a chave existe, a DANFSe pode ser gerada na hora, e
    não há nada "aguardando nacional" para um job buscar depois.

    Com `nacional=False` o XML é do modelo antigo (ABRASF). Esse caminho fica de
    pé porque a tela de recuperação ainda é usada em notas antigas.
    """
    ctx = ctx or preparar(card_id)
    card, obra, r = ctx["card"], ctx["obra"], ctx["r"]
    gc, cred = ctx["gc"], ctx["cred"]
    token = cred["PIPEFY_TOKEN"]
    discr_limpa = getattr(ctx.get("dados_rps"), "discriminacao", "") or ""
    obra_cod = card["codigo_obra"]
    med = card["numero_medicao"]
    ano = int(str(data_iso)[:4])
    data_br = f"{data_iso[8:10]}/{data_iso[5:7]}/{data_iso[:4]}"
    nome_base = f"NOTA FISCAL {numero} - {med} ª Medição {obra_cod}"
    planilha = gc.open_by_key(ID_PROC)

    with open(nota_xml_path, "rb") as fh:
        xml_bytes = fh.read()
    xml_texto = xml_bytes.decode("utf-8", "replace")
    xml_abrasf = None if nacional else xml_texto
    xml_nac = xml_texto if nacional else None

    modelo = "NACIONAL (DPS)" if nacional else "antigo (ABRASF)"
    print(f"\n===== CONCLUSÃO (IMEDIATA) DA NOTA {numero} ({obra_cod}) — modelo {modelo} =====")

    ws_notas = abrir_aba(planilha, ABA_NOTAS)
    if notas_bws.ja_existe(ws_notas, numero) and not forcar:
        print(f">>> A nota {numero} JÁ consta na Notas BWS — parece já concluída.")
        print(">>> Para rodar de novo, repita com FORCAR no fim. Abortado.")
        return

    # 1) NOTAS BWS
    try:
        grav = notas_bws.gravar_linha(ws_notas, card, obra, r, numero, data_iso)
        _med = notas_bws._num_medicao(card)
        _td = (card.get("tipo_documento") or "(vazio)")
        print(f"[1] Notas BWS .......... {'linha gravada (A–P)' if grav else 'já existia, pulei'} "
              f"| Nº Med.: {_med} | Tipo de Documento: {_td}")
    except Exception as e:
        print(f"[1] Notas BWS .......... ERRO: {e}")

    # 2) OMIE — retenções SEMPRE da medição integral, e só na PRIMEIRA nota válida;
    #    da 2ª nota parcial em diante, só acrescenta o número (mantém as retenções).
    try:
        integracao = card.get("omie_integracao", "")
        validas_antes = [x for x in validacao.slots_preenchidos(card) if x["valida"]]
        primeira = len(validas_antes) == 0
        if nota_substituida:
            # substituição: o documento (remover antigo + adicionar novo) é feito numa
            # ÚNICA chamada na etapa de substituição — aqui não tocamos no Omie.
            print(f"[2] Omie ............... substituição: documento tratado na etapa de "
                  f"substituição (chamada combinada, evita trava do Omie)")
        elif primeira:
            cat = parse_categoria(obra.tributacao)
            ov = overrides_do_card(card)
            r_integral = calcular(card["valor_medicao"], cat, aliquota_iss=obra.aliquota_iss,
                                  bdi_diferenciado=card["bdi"], iss_retido=True, overrides=ov)
            _, doc = omie.alterar_retencoes(cred, integracao, r_integral, numero)
            print(f"[2] Omie ............... 1ª nota: retenções da medição INTEGRAL "
                  f"(R$ {brl(r_integral.valor_total)}); documento = {doc}")
        else:
            _, doc = omie.adicionar_numero(cred, integracao, numero)
            print(f"[2] Omie ............... nota seguinte: só nº acrescentado "
                  f"(retenções mantidas); documento = {doc}")
    except Exception as e:
        print(f"[2] Omie ............... ERRO: {e}")

    # 3) PIPEFY: slots A–E + limpeza dos 12 campos
    try:
        slot = pf.detectar_slot(card["campos_raw"])
        mut = pf.montar_mutation(card["card_id"], slot, numero, data_br, r.valor_total, r.valor_liquido)
        pf.executar(mut, token)
        print(f"[3] Pipefy slots ....... slot {slot} preenchido + 12 campos limpos")
    except pf.TodasNotasPreenchidas:
        print("[3] Pipefy slots ....... 5 slots (A–E) cheios — pulei a atualização do card "
              "(a nota foi emitida e os demais efeitos seguem normalmente)")
    except Exception as e:
        print(f"[3] Pipefy slots ....... ERRO: {e}")

    # 4) DRIVE: arquiva o XML municipal
    xml_fid = ""
    _nome_xml = f"{nome_base} (XML Nacional).xml" if nacional else f"{nome_base} (XML).xml"
    try:
        xml_fid, _ = drive.enviar(_nome_xml, xml_bytes, "xml")
        print(f"[4] Drive XML .......... {_nome_xml} arquivado (id {xml_fid[:12]}...)")
    except Exception as e:
        print(f"[4] Drive XML .......... ERRO: {e}")

    # 5) RECIBO -> Drive
    link_rec = ""
    try:
        # BRUTO da NF lido do XML, nunca do card — o card já pode ter sido limpo.
        vbruto = (nota_municipal.valor_bruto_nf_nacional(xml_nac) if nacional
                  else nota_municipal.valor_bruto_nf(xml_abrasf))
        dados_rec = efeitos.dados_recibo(card, obra, r, numero, valor_bruto=vbruto)
        recibo.gerar_recibo_pdf(dados_rec, "recibo_tmp.pdf")
        with open("recibo_tmp.pdf", "rb") as fh:
            _, link_rec = drive.enviar(f"{nome_base} (Recibo).pdf", fh.read(), "pdf")
        print(f"[5] Recibo (Drive) ..... {link_rec}")
    except Exception as e:
        print(f"[5] Recibo (Drive) ..... ERRO: {e}")

    # 6) MUNICIPAL NF-e -> Drive. No modelo nacional ela já sai COM a chave de
    #    acesso (e com o QR), porque a chave existe desde a emissão.
    link_mun = ""
    try:
        nota_municipal.gerar_nota_municipal_pdf(
            xml_abrasf, "mun_tmp.pdf", xml_nacional=xml_nac,
            discriminacao=(None if nacional else discr_limpa))
        with open("mun_tmp.pdf", "rb") as fh:
            _, link_mun = drive.enviar(f"{nome_base} (NFS-e).pdf", fh.read(), "pdf")
        print(f"[6] Municipal (Drive) .. {link_mun}")
    except Exception as e:
        print(f"[6] Municipal (Drive) .. ERRO: {e}")

    # 6b) DANFSe NACIONAL -> Drive. Antes isto saía minutos depois, num job que
    #     ficava perguntando à SEFIN se a nota já havia subido. Com a emissão
    #     indo direto pelo nacional, o XML já está na mão: sai agora.
    link_nac = ""
    if nacional:
        try:
            import danfse
            danfse.gerar_danfse_pdf(xml_nac, "danfse_tmp.pdf")
            with open("danfse_tmp.pdf", "rb") as fh:
                _, link_nac = drive.enviar(f"{nome_base} (NFS-e Nacional).pdf", fh.read(), "pdf")
            print(f"[6b] Nacional (Drive) .. {link_nac}")
        except Exception as e:
            print(f"[6b] Nacional (Drive) .. ERRO: {e}")

    # 7) NOTAS BWS LINKS (municipal + recibo; coluna nacional recebe o link de
    #    BUSCA MANUAL como placeholder — o job nacional troca pelo link real depois)
    try:
        ws_links = abrir_aba(planilha, ABA_LINKS)
        import os as _os
        _base = _os.getenv("EMISSAO_NF_BASE_URL", "").rstrip("/")
        _tok = _os.getenv("EMISSAO_NF_TOKEN") or _os.getenv("EMISSAO_TOKEN") or ""
        # No modelo nacional o link do nacional já existe. No antigo, a coluna
        # recebia o link da busca manual como marcador até o job preenchê-la.
        _link_nac_col = link_nac or (f"{_base}/emissao/nacional?token={_tok}"
                                     if (_base and not nacional) else "")
        notas_bws.gravar_links(ws_links, numero, obra_cod, ano, nome_base,
                               link_municipal=link_mun, link_nacional=_link_nac_col,
                               link_recibo=link_rec)
        _oque = "municipal + nacional + recibo" if link_nac else "municipal + recibo"
        print(f"[7] Notas BWS Links .... linha gravada ({_oque})")
    except Exception as e:
        print(f"[7] Notas BWS Links .... ERRO: {e}")

    # 8) PIPEFY: Descrição com os links que existirem no topo
    try:
        if link_mun or link_rec or link_nac:
            pf.atualizar_descricao_links(card["card_id"], link_mun, link_nac, link_rec, token)
            n = sum(bool(x) for x in (link_mun, link_nac, link_rec))
            print(f"[8] Pipefy Descrição ... {n} link(s) no topo")
    except Exception as e:
        print(f"[8] Pipefy Descrição ... ERRO: {e}")

    # 9) WHATSAPP (texto + recibo)
    try:
        dest, aviso = efeitos._ler_destinatarios(gc)
        if aviso:
            print(f"[9] WhatsApp ........... {aviso}")
        if nota_substituida:
            msg = zapi.montar_mensagem_substituicao(
                obra_cod, med, brl(r.valor_total),
                card.get("periodo_ini", ""), card.get("periodo_fim", ""), numero, nota_substituida)
        else:
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
        print(f"[9] WhatsApp ........... {envios} envio(s)")
    except Exception as e:
        print(f"[9] WhatsApp ........... ERRO: {e}")

    # 10) CONTROLE NACIONAL. No modelo antigo a nota ficava numa fila, esperando
    #     um job encontrá-la no nacional. No modelo nacional ela JÁ está lá — o
    #     registro entra direto como concluído, para guardar a chave e para o job
    #     não sair procurando o que já foi achado.
    try:
        ctrl.registrar_pendente(planilha, {
            "numero": numero, "card_id": card_id, "cod_verif": codigo,
            "obra": obra_cod, "med": med, "ano": ano,
            "toma_cnpj": card.get("cnpj_contratante", "") or getattr(obra, "cnpj_cliente", ""),
            "vServ": float(r.valor_total), "dCompet": str(data_iso)[:7],
            "xml_abrasf_file_id": xml_fid, "link_mun": link_mun, "link_rec": link_rec,
        })
        if nacional:
            ctrl.marcar_concluido(planilha, numero, link_nac, link_mun, chave_nacional)
            print(f"[10] Controle Nacional . já CONCLUÍDA (chave "
                  f"{chave_nacional[-8:] if chave_nacional else '—'})")
        else:
            print(f"[10] Controle Nacional . aguardando nacional (o job completa)")
    except Exception as e:
        print(f"[10] Controle Nacional . ERRO: {e}")

    print("\n===== IMEDIATO FINALIZADO" + (" — a nota já está completa =====" if nacional
                                          else " — a nacional sai no job ====="))


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("uso: python concluir.py <card_id> <numero> <codigo> <data_iso> <nota_xml> [FORCAR]")
        sys.exit(1)
    forcar = len(sys.argv) > 6 and sys.argv[6].strip().upper() == "FORCAR"
    try:
        concluir(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], forcar=forcar)
    except Exception as e:
        print(f"\n>>> ERRO GERAL: {type(e).__name__}: {e}")
